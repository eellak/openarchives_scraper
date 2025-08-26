#!/usr/bin/env python3
"""Collect ALL EDM links across ALL collections, paging through every listing.

Outputs:
- Global links parquet: openarchives_data/all_links.parquet (or under --out-dir)
- Collection summary parquet: openarchives_data/collections_links_summary.parquet (or under --out-dir)
- Single pages parquet: openarchives_data/pages.parquet (one row per listing page; or under --out-dir)
- Rotating run log: logs/openarchives_collect_all_YYYYMMDD_HHMMSS.log (configurable via --log-dir)

Behavior:
- Concurrency 6 by default; 3s timeout per request; 0.2s wait between requests; resume enabled by default
- Abort early if HTTP 500 responses exceed 99% of listing requests
- Evidence-minded: record exact link extraction heuristic and total-pages source
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import time
from dataclasses import dataclass
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
import pandas as pd

from .parsers import (
    build_search_page_url,
    extract_document_links,
    parse_total_pages,
)


INPUT_COLLECTIONS_PARQUET = Path("openarchives_data/collections.parquet")
OUTPUT_LINKS_PARQUET = Path("openarchives_data/all_links.parquet")
OUTPUT_COLLECTIONS_SUMMARY_PARQUET = Path("openarchives_data/collections_links_summary.parquet")
OUTPUT_PAGES_PARQUET = Path("openarchives_data/pages.parquet")

DEFAULT_TIMEOUT_S = 3.0
DEFAULT_CONCURRENCY = 6
DEFAULT_MAX_500_RATIO = 0.99
DEFAULT_WAIT_S = 0.2
DEFAULT_THRESHOLD_START = 1000  # minimum number of non-retry page requests before applying 500-ratio abort
DEFAULT_LOG_DIR = Path("logs")


@dataclass
class PageRecord:
    collection_slug: str
    collection_url: str
    page_number: int
    total_pages: int
    page_url: str
    status_code: int
    elapsed_s: float
    num_links: int
    retries: int
    total_pages_source: Optional[str]
    links_selector: str
    snapshot_path: Optional[str] = None


def _httpx_timeout(seconds: float = DEFAULT_TIMEOUT_S) -> httpx.Timeout:
    return httpx.Timeout(connect=seconds, read=seconds, write=seconds, pool=seconds)


def _make_client(concurrency: int, timeout_s: float) -> httpx.AsyncClient:
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async def _log_request(request: httpx.Request):
        logging.info("[http] start %s %s", request.method, request.url)
    async def _log_response(response: httpx.Response):
        logging.info("[http] done  %s %s -> %s", response.request.method, response.request.url, response.status_code)
    return httpx.AsyncClient(
        http2=True,
        timeout=_httpx_timeout(timeout_s),
        limits=limits,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) "
                "Gecko/20100101 Firefox/141.0"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        event_hooks={"request": [_log_request], "response": [_log_response]},
    )


async def _fetch_listing_page(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    page_url: str,
    wait_s: float = DEFAULT_WAIT_S,
) -> Tuple[int, float, str]:
    async with semaphore:
        t0 = time.perf_counter()
        try:
            # Small wait to avoid overwhelming the server
            if wait_s and wait_s > 0:
                await asyncio.sleep(wait_s)
            resp = await client.get(page_url)
            return resp.status_code, (time.perf_counter() - t0), resp.text
        except Exception as exc:
            logging.warning("[fetch] error for %s: %s", page_url, exc)
            return -1, (time.perf_counter() - t0), ""


def _load_existing_pages_from_parquet(pages_path: Path, collection_slug: str) -> Tuple[Dict[int, Dict[str, int]], Optional[int]]:
    """Return (existing_pages, total_pages_from_records) from a single pages parquet.

    existing_pages: {page_number: {status_code, retries, total_pages}}
    """
    existing: Dict[int, Dict[str, int]] = {}
    total_pages_from_records: Optional[int] = None
    if not pages_path.exists():
        return existing, None
    try:
        dfp = pd.read_parquet(pages_path)
    except Exception:
        return existing, None
    if dfp.empty:
        return existing, None
    sub = dfp[dfp["collection_slug"] == collection_slug]
    if sub.empty:
        return existing, None
    for _, rec in sub.iterrows():
        try:
            pn = int(rec.get("page_number", 0))
            if pn <= 0:
                continue
            status_code = int(rec.get("status_code", -1))
            retries = int(rec.get("retries", 0))
            tp = rec.get("total_pages", None)
            if pd.notna(tp):
                try:
                    tp_i = int(tp)
                    total_pages_from_records = max(total_pages_from_records or 0, tp_i)
                except Exception:
                    pass
            existing[pn] = {"status_code": status_code, "retries": retries, "total_pages": int(tp) if pd.notna(tp) else -1}
        except Exception:
            continue
    return existing, total_pages_from_records


async def run(
    in_parquet: Path,
    out_links: Path,
    out_summary: Path,
    out_pages: Path,
    concurrency: int,
    timeout_s: float,
    wait_s: float,
    max_500_ratio: float,
    threshold_start: int,
    max_collections: Optional[int],
    retries: int,
    resume: bool,
    verify_sentinel: bool,
    sentinel_page: int,
    snapshots_dir: Optional[Path],
    progress: bool,
    pages_estimate: Optional[int],
) -> None:
    df = pd.read_parquet(in_parquet)
    if not {"collection_url", "collection_slug"}.issubset(df.columns):
        raise SystemExit("Input parquet must contain collection_url and collection_slug")

    # Prepare outputs
    out_links.parent.mkdir(parents=True, exist_ok=True)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_pages.parent.mkdir(parents=True, exist_ok=True)

    # Global counters for abort policy
    total_listing_requests = 0
    total_500 = 0
    counters_lock = asyncio.Lock()

    # Global counters for attempt stats (include retries)
    attempt_total_requests = 0
    attempt_500 = 0

    # Global accumulators
    global_link_rows: List[Dict[str, object]] = []
    collections_summary_rows: List[Dict[str, object]] = []
    global_pages_rows: List[Dict[str, object]] = []

    async with _make_client(concurrency, timeout_s) as client:
        semaphore = asyncio.Semaphore(concurrency)
        start_ts = time.perf_counter()
        grand_planned_pages = 0
        grand_completed_pages = 0
        # Base completed pages from previous runs (for resume-aware progress)
        base_completed_pages = 0
        try:
            if out_pages.exists():
                df_prev = pd.read_parquet(out_pages)
                if not df_prev.empty and {"collection_slug", "page_number", "status_code"}.issubset(df_prev.columns):
                    df_prev_ok = df_prev[df_prev["status_code"] == 200]
                    if not df_prev_ok.empty:
                        df_prev_ok = df_prev_ok.drop_duplicates(subset=["collection_slug", "page_number"], keep="last")
                        base_completed_pages = int(len(df_prev_ok))
        except Exception:
            base_completed_pages = 0

        def _render_progress() -> None:
            if not progress:
                return
            # Cumulative percent/remaining; speed based on this run only
            dynamic_total = base_completed_pages + grand_planned_pages
            total = (pages_estimate if (pages_estimate and pages_estimate > 0) else dynamic_total)
            processed = base_completed_pages + grand_completed_pages
            elapsed = max(1e-6, time.perf_counter() - start_ts)
            speed = (grand_completed_pages / elapsed) if elapsed > 0 else 0.0
            # 500 ratio tracks only new-page fetches (same as abort logic)
            # Show attempts ratio (includes retries) per user intent
            try:
                current_ratio = (attempt_500 / max(1, attempt_total_requests)) * 100.0
            except Exception:
                current_ratio = 0.0
            # Progress bar
            width = 40
            frac = 0.0 if total <= 0 else min(1.0, processed / max(1, total))
            filled = int(width * frac)
            bar = f"[{'#'*filled}{'.'*(width-filled)}]"
            # ETA
            remaining = max(0.0, (total - processed))
            eta_s = (remaining / speed) if speed > 0 else 0.0
            eta_min = int(eta_s // 60)
            eta_sec = int(eta_s % 60)
            line = (
                f"{bar} {processed}/{total} pages | {speed:.1f} p/s | "
                f"500s: {attempt_500}/{max(1,attempt_total_requests)} ({current_ratio:.1f}%) | "
                f"ETA: {eta_min:02d}:{eta_sec:02d}"
            )
            try:
                sys.stdout.write("\r" + line)
                sys.stdout.flush()
            except Exception:
                pass

        async def _fetch_first_and_total_pages(collection_url: str) -> Tuple[int, str, Optional[str], str]:
            first_url = build_search_page_url(collection_url, 1)
            status, elapsed, html = await _fetch_listing_page(client, semaphore, first_url, wait_s)
            nonlocal total_listing_requests, total_500, attempt_total_requests, attempt_500
            async with counters_lock:
                total_listing_requests += 1
                if status == 500:
                    total_500 += 1
                attempt_total_requests += 1
                if status == 500:
                    attempt_500 += 1
            _render_progress()
            total_pages = parse_total_pages(html) or 1
            # Infer total_pages_source with explicit JS markers first
            import re
            source: Optional[str] = None
            raw = html or ""
            if re.search(r"var\s+maxPages\s*=\s*['\"](\d+)['\"]\s*;", raw):
                source = "js:maxPages"
            elif re.search(r"var\s+itemsNum\s*=\s*['\"](\d+)['\"]\s*;", raw) and re.search(r"var\s+pageSize\s*=\s*['\"](\d+)['\"]\s*;", raw):
                source = "js:itemsNum/pageSize"
            else:
                text = raw
                if "pages" in text.lower():
                    source = "pages_text"
                elif "τεκμήρια" in text:
                    source = "τεκμήρια"
            return total_pages, first_url, source, html

        # Iterate collections sequentially to apply abort policy early
        processed = 0
        for idx, row in df.iterrows():
            if max_collections is not None and processed >= max_collections:
                break
            collection_url = str(row["collection_url"]).strip()
            collection_slug = str(row["collection_slug"]).strip()
            claimed_count = None
            if "item_count" in df.columns and not pd.isna(row.get("item_count")):
                try:
                    claimed_count = int(row.get("item_count"))
                except Exception:
                    claimed_count = None

            logging.info("[collection] %s → %s", collection_slug, collection_url)
            # removed special START logging; reverting to original per-request verbosity

            # If resume, load existing pages from parquet first
            existing_pages, existing_total_pages = _load_existing_pages_from_parquet(out_pages, collection_slug)

            # Determine total pages and source
            # Do NOT refresh page-1 on resume; use recorded total_pages. Fetch first page only for fresh runs.
            if resume and existing_total_pages:
                total_pages = int(existing_total_pages)
                first_url = build_search_page_url(collection_url, 1)
                total_pages_source = "resume_existing_pages"
                first_html = None
            else:
                total_pages, first_url, total_pages_source, first_html = await _fetch_first_and_total_pages(collection_url)

            # Optional sentinel verification and snapshotting
            sentinel_snapshot_path: Optional[str] = None
            first_snapshot_path: Optional[str] = None
            if verify_sentinel:
                # Save first page HTML if available
                if snapshots_dir:
                    snapshots_dir.mkdir(parents=True, exist_ok=True)
                try:
                    if first_html and snapshots_dir:
                        first_snapshot_path = str((snapshots_dir / f"{collection_slug}_search_p1.html").resolve())
                        Path(first_snapshot_path).write_text(first_html, encoding="utf-8")
                except Exception:
                    pass
                # Fetch sentinel page
                sentinel_url = build_search_page_url(collection_url, int(sentinel_page))
                s_status, s_elapsed, s_html = await _fetch_listing_page(client, semaphore, sentinel_url, wait_s)
                try:
                    if snapshots_dir and s_html:
                        sentinel_snapshot_path = str((snapshots_dir / f"{collection_slug}_search_p{int(sentinel_page)}.html").resolve())
                        Path(sentinel_snapshot_path).write_text(s_html, encoding="utf-8")
                except Exception:
                    pass
                # Log comparison summary (no guessing: count explicit edm links and JS counts)
                import re as _re
                def _count_edm(html: str) -> int:
                    from bs4 import BeautifulSoup as _BS
                    soup = _BS(html or "", "html.parser")
                    return sum(1 for a in soup.find_all("a", href=True) if "/aggregator-openarchives/edm/" in a["href"])    
                def _find_js_int(var: str, html: str) -> Optional[int]:
                    m = _re.search(rf"var\\s+{var}\\s*=\\s*['\"](\\d+)['\"]\\s*;", html or "")
                    if m:
                        try:
                            return int(m.group(1))
                        except Exception:
                            return None
                    return None
                def _has_pagination(html: str) -> bool:
                    if not html:
                        return False
                    # Look for twbsPagination init or pagination containers
                    return ("twbsPagination" in html) or ("id=\"topPagination\"" in html) or ("id=\"pagination2\"" in html)
                def _count_cards(html: str) -> int:
                    from bs4 import BeautifulSoup as _BS
                    soup = _BS(html or "", "html.parser")
                    return len(soup.select(".edm-entity-result"))
                n1 = _count_edm(first_html or "") if first_html else None
                n2 = _count_edm(s_html or "") if s_html else None
                i1 = _find_js_int("itemsNum", first_html or "") if first_html else None
                i2 = _find_js_int("itemsNum", s_html or "") if s_html else None
                m1 = _find_js_int("maxPages", first_html or "") if first_html else None
                m2 = _find_js_int("maxPages", s_html or "") if s_html else None
                p1_summary = {
                    "page": 1,
                    "num_edm_links": n1,
                    "num_cards": _count_cards(first_html or ""),
                    "has_pagination": _has_pagination(first_html or ""),
                    "itemsNum_js": i1,
                    "maxPages_js": m1,
                    "snapshot_path": first_snapshot_path,
                }
                pN_summary = {
                    "page": int(sentinel_page),
                    "num_edm_links": n2,
                    "num_cards": _count_cards(s_html or ""),
                    "has_pagination": _has_pagination(s_html or ""),
                    "itemsNum_js": i2,
                    "maxPages_js": m2,
                    "snapshot_path": sentinel_snapshot_path,
                }
                logging.info("[sentinel] %s p1: links=%s cards=%s pag=%s | p%d: links=%s cards=%s pag=%s",
                             collection_slug,
                             p1_summary["num_edm_links"], p1_summary["num_cards"], p1_summary["has_pagination"],
                             int(sentinel_page),
                             pN_summary["num_edm_links"], pN_summary["num_cards"], pN_summary["has_pagination"],
                )
                # Persist diff report under out directory
                try:
                    results_dir = out_links.parent if out_links else Path(".")
                    diff_path = results_dir / f"sentinel_diff_{collection_slug}_p1_vs_p{int(sentinel_page)}.json"
                    diff_payload = {"collection_slug": collection_slug, "p1": p1_summary, "pN": pN_summary}
                    diff_path.write_text(json.dumps(diff_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass

            # Abort policy check
            async with counters_lock:
                ratio = (total_500 / max(1, total_listing_requests))
                apply_abort = total_listing_requests >= max(0, int(threshold_start))
            if apply_abort and ratio > max_500_ratio:
                logging.warning("500-ratio %.1f%% exceeded threshold %.1f%%; aborting",
                                ratio * 100.0, max_500_ratio * 100.0)
                break

            # Prepare worklists
            # Early-stop retry policy: attempt up to `retries` times per failing page, but stop as soon as a 200 is received
            retry_targets: List[Tuple[int, int]] = []  # (page_number, prev_retries)
            if resume and retries > 0 and existing_pages:
                for pn, rec in sorted(existing_pages.items()):
                    sc = int(rec.get("status_code", -1))
                    prev_r = int(rec.get("retries", 0))
                    if sc != 200:
                        retry_targets.append((pn, prev_r))

            # Continue from last page scanned
            if resume and existing_pages:
                max_done = max(existing_pages.keys())
                # If max_done >= total_pages, nothing new to fetch
                if max_done >= int(total_pages):
                    new_pages = []
                else:
                    new_pages = list(range(max_done + 1, max(1, int(total_pages)) + 1))
            else:
                new_pages = list(range(1, max(1, int(total_pages)) + 1))

            # Add planned pages to grand total and render progress
            planned = len(retry_targets) + len(new_pages)
            grand_planned_pages += planned
            _render_progress()

            # Container for per-page results of this collection
            per_collection_page_rows: List[PageRecord] = []

            async def _process_page(page_number: int, retries_value: int) -> int:
                url = build_search_page_url(collection_url, page_number)
                status, elapsed, html = await _fetch_listing_page(client, semaphore, url, wait_s)
                # Only count new-page fetches towards abort ratio (not retries)
                nonlocal total_listing_requests, total_500, attempt_total_requests, attempt_500
                if retries_value == 0:
                    async with counters_lock:
                        total_listing_requests += 1
                        if status == 500:
                            total_500 += 1
                # Always count attempts (includes retries)
                async with counters_lock:
                    attempt_total_requests += 1
                    if status == 500:
                        attempt_500 += 1
                links = extract_document_links(html, url) if (status == 200 and html) else []
                # Append link rows
                for edm in links:
                    global_link_rows.append({
                        "collection_slug": collection_slug,
                        "collection_url": collection_url,
                        "page_number": page_number,
                        "page_url": url,
                        "edm_url": edm,
                    })
                # Update completed pages and progress
                async with counters_lock:
                    nonlocal grand_completed_pages
                    grand_completed_pages += 1
                _render_progress()
                per_collection_page_rows.append(PageRecord(
                    collection_slug=collection_slug,
                    collection_url=collection_url,
                    page_number=page_number,
                    total_pages=int(total_pages),
                    page_url=url,
                    status_code=int(status),
                    elapsed_s=float(elapsed),
                    num_links=len(links),
                    retries=int(retries_value),
                    total_pages_source=total_pages_source,
                    links_selector="href*=/aggregator-openarchives/edm/",
                    snapshot_path=(first_snapshot_path if (page_number == 1 and first_snapshot_path) else None),
                ))
                return int(status)

            # First wave: retries for previously failed pages (if enabled) with early-stop per page
            if retry_targets:
                async def _retry_until_success(page_number: int, prev_retries_value: int) -> None:
                    for add in range(1, retries + 1):
                        status_code_try = await _process_page(page_number, prev_retries_value + add)
                        if status_code_try == 200:
                            break
                retry_tasks = [asyncio.create_task(_retry_until_success(pn, prev_r)) for pn, prev_r in retry_targets]
                for coro in asyncio.as_completed(retry_tasks):
                    try:
                        await coro
                    except asyncio.CancelledError:
                        pass
                # No abort threshold checks during retries

            # Second wave: new pages (either all pages or continuation)
            if new_pages:
                page_tasks = [asyncio.create_task(_process_page(n, 0)) for n in new_pages]
                for coro in asyncio.as_completed(page_tasks):
                    try:
                        await coro
                    except asyncio.CancelledError:
                        pass
                    # Check abort threshold live
                    async with counters_lock:
                        ratio = (total_500 / max(1, total_listing_requests))
                        apply_abort = total_listing_requests >= max(0, int(threshold_start))
                    if apply_abort and ratio > max_500_ratio:
                        logging.warning("500-ratio %.1f%% exceeded threshold %.1f%%; aborting further pages",
                                        ratio * 100.0, max_500_ratio * 100.0)
                        # Cancel remaining tasks for this collection
                        for task in page_tasks:
                            if not task.done():
                                task.cancel()
                        break

            # Accumulate per-page rows into global pages list
            for rec in per_collection_page_rows:
                global_pages_rows.append({
                    "collection_slug": rec.collection_slug,
                    "collection_url": rec.collection_url,
                    "page_number": rec.page_number,
                    "total_pages": rec.total_pages,
                    "page_url": rec.page_url,
                    "status_code": rec.status_code,
                    "elapsed_s": rec.elapsed_s,
                    "num_links": rec.num_links,
                    "retries": rec.retries,
                    "total_pages_source": rec.total_pages_source,
                    "links_selector": rec.links_selector,
                    "snapshot_path": rec.snapshot_path,
                })

            # If we captured sentinel, record a page row for it (retries=-1 so real fetch wins in merges)
            if verify_sentinel and sentinel_snapshot_path is not None:
                sentinel_url = build_search_page_url(collection_url, int(sentinel_page))
                global_pages_rows.append({
                    "collection_slug": collection_slug,
                    "collection_url": collection_url,
                    "page_number": int(sentinel_page),
                    "total_pages": int(total_pages),
                    "page_url": sentinel_url,
                    "status_code": -1,
                    "elapsed_s": 0.0,
                    "num_links": 0,
                    "retries": -1,
                    "total_pages_source": total_pages_source,
                    "links_selector": "href*=/aggregator-openarchives/edm/",
                    "snapshot_path": sentinel_snapshot_path,
                })

            # Build per-collection summary
            total_found = sum(p.num_links for p in per_collection_page_rows)
            pages_attempted = len(per_collection_page_rows)
            pages_200 = sum(1 for p in per_collection_page_rows if p.status_code == 200)
            pages_500 = sum(1 for p in per_collection_page_rows if p.status_code == 500)
            collections_summary_rows.append({
                "collection_slug": collection_slug,
                "collection_url": collection_url,
                "claimed_item_count": claimed_count,
                "detected_total_pages": int(total_pages),
                "pages_attempted": pages_attempted,
                "pages_ok": pages_200,
                "pages_500": pages_500,
                "links_found": int(total_found),
            })

            # removed special DONE logging; original logging kept

            processed += 1

            # Abort global if threshold exceeded
            async with counters_lock:
                ratio = (total_500 / max(1, total_listing_requests))
                apply_abort = total_listing_requests >= max(0, int(threshold_start))
            if apply_abort and ratio > max_500_ratio:
                break

    # Write/merge global links parquet
    links_new = pd.DataFrame.from_records(global_link_rows)
    if out_links.exists():
        try:
            links_old = pd.read_parquet(out_links)
            if not links_old.empty:
                key = ["collection_slug", "page_number", "edm_url"]
                merged_links = pd.concat([links_old, links_new], ignore_index=True)
                merged_links.sort_values(key, inplace=True)
                merged_links = merged_links.drop_duplicates(subset=key, keep="last")
                merged_links.to_parquet(out_links, index=False)
            else:
                links_new.to_parquet(out_links, index=False)
        except Exception:
            links_new.to_parquet(out_links, index=False)
    else:
        links_new.to_parquet(out_links, index=False)
    logging.info("Wrote %s link rows to %s", len(links_new), out_links)

    # Merge and write pages parquet (prefer best status: 200 > others; then highest retries; latest attempts win among failures)
    pages_new = pd.DataFrame.from_records(global_pages_rows)
    if out_pages.exists():
        try:
            pages_old = pd.read_parquet(out_pages)
            if not pages_old.empty:
                key = ["collection_slug", "page_number"]
                merged = pd.concat([pages_old, pages_new], ignore_index=True)
                # Status order: 200 wins over any non-200. Among non-200, keep latest attempt (highest retries)
                if "status_code" in merged.columns:
                    merged["_status_order"] = merged["status_code"].apply(lambda s: 1 if s == 200 else 0)
                else:
                    merged["_status_order"] = 0
                if "retries" in merged.columns:
                    merged["retries"] = merged["retries"].fillna(0).astype(int)
                else:
                    merged["retries"] = 0
                merged.sort_values(key + ["_status_order", "retries"], inplace=True)
                merged = merged.drop_duplicates(subset=key, keep="last").drop(columns=["_status_order"], errors="ignore")
                merged.to_parquet(out_pages, index=False)
            else:
                pages_new.to_parquet(out_pages, index=False)
        except Exception:
            pages_new.to_parquet(out_pages, index=False)
    else:
        pages_new.to_parquet(out_pages, index=False)
    logging.info("Wrote %s page rows to %s", len(pages_new), out_pages)

    # Write/merge collection summaries; keep max across numeric counters per collection
    summary_new = pd.DataFrame.from_records(collections_summary_rows)
    if out_summary.exists():
        try:
            summary_old = pd.read_parquet(out_summary)
            if not summary_old.empty:
                combined = pd.concat([summary_old, summary_new], ignore_index=True)
                # Group by collection_slug and keep the max for numeric columns; first valid for url/name
                non_numeric = ["collection_url", "claimed_item_count"]
                numeric_cols = [c for c in combined.columns if c not in ["collection_slug"] + non_numeric]
                agg_dict = {c: "max" for c in numeric_cols}
                agg_dict.update({"collection_url": "first", "claimed_item_count": "first"})
                merged_summary = combined.groupby("collection_slug", as_index=False).agg(agg_dict)
                merged_summary.to_parquet(out_summary, index=False)
            else:
                summary_new.to_parquet(out_summary, index=False)
        except Exception:
            summary_new.to_parquet(out_summary, index=False)
    else:
        summary_new.to_parquet(out_summary, index=False)
    logging.info("Wrote %s collection summaries to %s", len(collections_summary_rows), out_summary)
    # Finish progress line with newline
    try:
        sys.stdout.write("\n")
        sys.stdout.flush()
    except Exception:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect EDM links across all collections with pagination")
    parser.add_argument("--in", dest="in_parquet", default=str(INPUT_COLLECTIONS_PARQUET), help="Input collections parquet (read-only)")
    parser.add_argument("--out-links", default=str(OUTPUT_LINKS_PARQUET), help="Output global links parquet path (overridden by --out-dir)")
    parser.add_argument("--out-summary", default=str(OUTPUT_COLLECTIONS_SUMMARY_PARQUET), help="Output collections summary parquet path (overridden by --out-dir)")
    parser.add_argument("--pages", dest="out_pages", default=str(OUTPUT_PAGES_PARQUET), help="Output pages parquet path (overridden by --out-dir)")
    parser.add_argument("--out-dir", default="openarchives_results", help="Directory to write outputs (links, pages, summary)")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="Per-request timeout in seconds (no retries)")
    parser.add_argument("--max-500-ratio", type=float, default=DEFAULT_MAX_500_RATIO, help="Abort when 500 responses exceed this ratio")
    parser.add_argument("--threshold-start", type=int, default=DEFAULT_THRESHOLD_START, help="Minimum number of non-retry page requests before applying the 500%% abort check")
    parser.add_argument("--max-collections", type=int, default=None, help="Optional cap for quick verification")
    parser.add_argument("--retries", type=int, default=0, help="Retry count for previously failed pages (resume mode)")
    parser.add_argument("--resume", action="store_true", default=True, help="Resume from existing per-page parquet state and append new pages")
    parser.add_argument("--verify-sentinel", action="store_true", help="Fetch and save an out-of-range sentinel page (e.g., 9999) for diffing")
    parser.add_argument("--sentinel-page", type=int, default=9999, help="Out-of-range page number to fetch for comparison")
    parser.add_argument("--snapshots-dir", default="snapshots/openarchives/listings", help="Directory to store listing HTML snapshots for verification")
    parser.add_argument("--progress", action="store_true", help="Show a terminal progress bar with speed and 500% ratio")
    parser.add_argument("--pages-estimate", type=int, default=None, help="Estimated total pages to traverse (for progress bar and ETA)")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR), help="Directory to write an info-level run log with full request traces")
    parser.add_argument("--wait", type=float, default=DEFAULT_WAIT_S, help="Seconds to sleep before each request to throttle load (per task)")

    args = parser.parse_args()
    # Configure console + file logging (full logs to file)
    ts = time.strftime("%Y%m%d_%H%M%S")
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    logfile = log_dir / f"openarchives_collect_all_{ts}.log"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    # Clear any existing handlers (to avoid duplication on repeated runs in same interpreter)
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    fileh = logging.FileHandler(str(logfile), encoding="utf-8")
    fileh.setLevel(logging.INFO)
    fileh.setFormatter(formatter)
    root_logger.addHandler(console)
    root_logger.addHandler(fileh)
    logging.info("Log file: %s", logfile)
    t0 = time.time()
    # Compute effective output paths
    out_links_path = Path(args.out_links)
    out_summary_path = Path(args.out_summary)
    out_pages_path = Path(args.out_pages)
    if args.out_dir:
        base = Path(args.out_dir)
        base.mkdir(parents=True, exist_ok=True)
        out_links_path = base / OUTPUT_LINKS_PARQUET.name
        out_summary_path = base / OUTPUT_COLLECTIONS_SUMMARY_PARQUET.name
        out_pages_path = base / OUTPUT_PAGES_PARQUET.name
    asyncio.run(run(
        Path(args.in_parquet),
        out_links_path,
        out_summary_path,
        out_pages_path,
        args.concurrency,
        args.timeout,
        float(args.wait),
        args.max_500_ratio,
        int(args.threshold_start),
        args.max_collections,
        args.retries,
        True if args.resume else False,
        args.verify_sentinel,
        int(args.sentinel_page),
        Path(args.snapshots_dir) if args.snapshots_dir else None,
        True if args.progress else False,
        int(args.pages_estimate) if args.pages_estimate else 25979,
    ))
    logging.info("Done in %.3fs", time.time() - t0)


if __name__ == "__main__":
    main()


