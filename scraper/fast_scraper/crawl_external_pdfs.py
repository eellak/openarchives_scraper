#!/usr/bin/env python3
"""Crawl external item pages and extract in-order PDF links.

Modeled on fast_scraper/fetch_edm_metadata.py for efficiency:
- Async httpx with HTTP/2, pooled connections, semaphore concurrency
- Minimal HTML parsing using a single pass regex over href/src attributes
- Deduplicate while preserving first-seen (top-to-bottom) order

Input Parquets:
- edm_metadata_labeled.parquet: expects columns
  [collection_slug, collection_url, edm_url, external_link, language_code, page_number, type]
- collections_links_enriched_ge1k_sorted.parquet: expects columns
  [_slug, url, item_count, pct_ELL, pct_NONE]

Selection criteria:
- Only rows where collection_slug in collections with item_count >= 1000 AND (pct_ELL>0 or pct_NONE>0)
- Only rows where language_code in {"ELL", "NONE"}
- Only rows with a non-empty external_link

Output Parquet columns:
- collection_slug, collection_url, page_number, edm_url, external_link, language_code, type
- status_code, elapsed_s, retries
- pdf_links_json (JSON-encoded list[str] in top-to-bottom order), pdf_links_count

CLI example:
  python -m scraper.fast_scraper.crawl_external_pdfs \
    --edm /mnt/data/openarchive_scraper/openarchives_results/edm_metadata_labeled.parquet \
    --collections /mnt/data/openarchive_scraper/openarchives_results/collections_links_enriched_ge1k_sorted.parquet \
    --out /mnt/data/openarchive_scraper/openarchives_results/external_pdfs.parquet \
    --concurrency 20 --timeout 2.0 --progress
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import logging
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from collections import defaultdict
import csv
import random
from email.utils import parsedate_to_datetime

import httpx
import pandas as pd
from urllib.parse import urljoin, urlparse
from .user_agents import random_user_agent, deterministic_user_agent


DEFAULT_CONCURRENCY = 20
DEFAULT_TIMEOUT_S = 2.0
DEFAULT_WAIT_S = 0.0


@dataclass
class Row:
    collection_slug: str
    collection_url: str
    page_number: int
    edm_url: str
    external_link: str
    language_code: str
    type: str


def _httpx_timeout(seconds: float) -> httpx.Timeout:
    return httpx.Timeout(connect=seconds, read=seconds, write=seconds, pool=seconds)


def _select_collections(df_collections: pd.DataFrame) -> List[str]:
    # Safety: missing columns default to false/zero
    df = df_collections.copy()
    for col in ["item_count", "pct_ELL", "pct_NONE"]:
        if col not in df.columns:
            df[col] = 0.0
    mask = (df["item_count"].fillna(0) >= 1000) & (
        (df["pct_ELL"].fillna(0) > 0) | (df["pct_NONE"].fillna(0) > 0)
    )
    slugs = df.loc[mask, "_slug"].dropna().astype(str).unique().tolist()
    return slugs


_HREF_SRC_RE = re.compile(
    r"""(?:href|src)\s*=\s*(?:
            [\"']([^\"']+)[\"']   # quoted
        |   ([^\s>\"']+)            # or unquoted until delimiter
        )""",
    re.IGNORECASE | re.VERBOSE,
)


def _is_pdf_url(url: str) -> bool:
    try:
        p = urlparse(url)
        path = (p.path or "").lower()
        return path.endswith(".pdf")
    except Exception:
        return False


def extract_pdf_links_in_order(html: str, base_url: str) -> List[str]:
    """Find .pdf links in the order they appear in the HTML.

    - Scans href/src attributes once; resolves relative URLs
    - Keeps the first occurrence of each absolute URL to preserve order
    - Ignores malformed URLs and non-PDF resources
    """
    if not html:
        return []
    seen = set()
    ordered: List[str] = []
    for m in _HREF_SRC_RE.finditer(html):
        raw = (m.group(1) or m.group(2) or "").strip()
        if not raw:
            continue
        # Skip anchors and javascript:void
        if raw.startswith("#") or raw.lower().startswith("javascript:"):
            continue
        abs_url = urljoin(base_url, raw)
        if not _is_pdf_url(abs_url):
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)
        ordered.append(abs_url)
    return ordered


def _parse_retry_after(val: str) -> float:
    """Parse Retry-After header to seconds.

    Supports integer seconds or HTTP-date; returns 0 on failure.
    """
    if not val:
        return 0.0
    try:
        # Integer seconds
        return float(int(val))
    except Exception:
        try:
            dt = parsedate_to_datetime(val)
            if dt is None:
                return 0.0
            return max(0.0, (dt - parsedate_to_datetime(time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()))).total_seconds())
        except Exception:
            return 0.0


async def _fetch_page(
    client: httpx.AsyncClient,
    url: str,
    *,
    semaphore: Optional[asyncio.Semaphore] = None,
    wait_s: float = 0.0,
    ua_mode: str = "fixed",
) -> Tuple[int, float, str]:
    async def _get() -> httpx.Response:
        # Add small jitter to avoid synchronized bursts
        if wait_s and wait_s > 0:
            j = wait_s * (1.0 + random.uniform(-0.2, 0.5))
            if j > 0:
                await asyncio.sleep(j)
        if ua_mode == "random":
            ua = random_user_agent()
        elif ua_mode == "per_domain":
            try:
                ua = deterministic_user_agent(urlparse(url).netloc or "")
            except Exception:
                ua = random_user_agent()
        else:
            ua = "openarchives-fast-crawler/1.0 (+https://www.openarchives.gr)"
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Accept-Language": "el, en;q=0.8",
            "Referer": f"{urlparse(url).scheme}://{urlparse(url).netloc}/",
        }
        resp = await client.get(url, headers=headers)
        # Respect Retry-After for 429/503; add decorrelated backoff
        if resp.status_code in (429, 503):
            ra = 0.0
            try:
                ra = _parse_retry_after(resp.headers.get('Retry-After', ''))
            except Exception:
                ra = 0.0
            # Fallback to jittered wait if no RA
            if ra <= 0 and wait_s and wait_s > 0:
                ra = wait_s * (1.0 + random.uniform(0.0, 1.0))
            if ra and ra > 0:
                try:
                    await asyncio.sleep(min(60.0, float(ra)))
                except Exception:
                    pass
        return resp

    t0 = time.perf_counter()
    if semaphore is None:
        resp = await _get()
    else:
        async with semaphore:
            resp = await _get()
    elapsed = time.perf_counter() - t0
    text = resp.text if resp.status_code == 200 else ""
    return int(resp.status_code), float(elapsed), text


async def run(
    edm_parquet: Path,
    collections_parquet: Path,
    out_parquet: Path,
    limit: int,
    concurrency: int,
    timeout_s: float,
    wait_s: float,
    progress: bool,
    compression: Optional[str],
    retries: int,
    resume: bool,
    retry_only: bool,
    metrics_chunk: int = 100,
    metrics_out: Optional[Path] = None,
    skip_collections: Optional[List[str]] = None,
    checkpoint_every: int = 0,
    checkpoint_seconds: float = 0.0,
    timeouts_streak_threshold: int = 0,
    baseline_parquet: Optional[Path] = None,
    only_collections: Optional[List[str]] = None,
) -> None:
    # Logging setup (may be overridden by main via --log-level)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    log = logging.getLogger("crawler")

    # Load inputs
    col_req = ["_slug", "url", "item_count", "pct_ELL", "pct_NONE"]
    # Read collections parquet once and select needed columns
    df_all_cols = pd.read_parquet(collections_parquet)
    have_cols = [c for c in col_req if c in df_all_cols.columns]
    df_cols = df_all_cols[have_cols].copy() if have_cols else pd.DataFrame(columns=col_req)
    selected_slugs = set(_select_collections(df_cols))
    if not selected_slugs:
        log.warning("No collections selected by criteria; exiting with no work.")
        return

    req_cols = [
        "collection_slug",
        "collection_url",
        "page_number",
        "edm_url",
        "external_link",
        "language_code",
        "type",
    ]
    df = pd.read_parquet(edm_parquet, columns=req_cols)
    # Filter per spec
    df = df[(df["collection_slug"].isin(selected_slugs)) & (df["language_code"].isin(["ELL", "NONE"])) & (df["external_link"].notna()) & (df["external_link"].astype(str).str.len() > 0)]
    # Optional include-only collections filter
    if only_collections:
        only_set = set([s.strip() for s in only_collections if s and s.strip()])
        if only_set:
            df = df[df["collection_slug"].isin(only_set)]

    # Skip specific collections by marking them as timeouts (status_code=-1) without fetching.
    skip_set: set = set([s.strip() for s in (skip_collections or []) if s and s.strip()])
    df_skip = df[df["collection_slug"].isin(skip_set)].copy() if skip_set else df.iloc[0:0].copy()
    df = df[~df["collection_slug"].isin(skip_set)] if skip_set else df
    if limit and limit > 0:
        df = df.head(limit)

    if df.empty:
        log.info("No rows after filtering; nothing to crawl.")
        return

    # Unique external links to crawl (exclude skipped)
    ext_links = df.dropna(subset=["external_link"])["external_link"].astype(str)
    unique_links = ext_links.drop_duplicates().tolist()
    total_unique = len(unique_links)
    log.info("Planning crawl: %s unique external pages from %s filtered items", total_unique, len(df))

    # Prepare resume/baseline state
    combined_status: Dict[str, int] = {}
    prev_retries_by_url: Dict[str, int] = {}
    # Baseline from a master parquet (skip already-200 entries globally)
    if baseline_parquet and baseline_parquet.exists():
        try:
            base = pd.read_parquet(baseline_parquet, columns=["external_link", "status_code", "retries"]).copy()
            if not base.empty:
                base = base.dropna(subset=["external_link"]).copy()
                # Keep last occurrence per URL, prefer 200 when present
                base["_ok"] = (base["status_code"] == 200).astype(int)
                base.sort_values(["external_link", "_ok"], inplace=True)
                bestb = base.drop_duplicates(subset=["external_link"], keep="last")
                combined_status.update(bestb.set_index("external_link")["status_code"].to_dict())
                # Track previous retries (max seen per URL)
                if "retries" in bestb.columns:
                    prev_retries_by_url.update(bestb.set_index("external_link")["retries"].fillna(0).astype(int).to_dict())
        except Exception:
            pass
    # Add current out_parquet state if resuming
    if out_parquet.exists():
        try:
            old = pd.read_parquet(out_parquet, columns=["external_link", "status_code", "retries"]).copy()
            if not old.empty:
                old["_ok"] = (old["status_code"] == 200).astype(int)
                old["retries"] = old.get("retries", 0).fillna(0).astype(int)
                old.sort_values(["external_link", "_ok", "retries"], inplace=True)
                best = old.drop_duplicates(subset=["external_link"], keep="last")
                # Update/override baseline with latest
                combined_status.update(best.set_index("external_link")["status_code"].to_dict())
                prev_retries_by_url.update(best.set_index("external_link")["retries"].to_dict())
        except Exception:
            pass

    if resume:
        if retries > 0:
            base = [u for u in unique_links if (u in combined_status)] if retry_only else unique_links
            work_links = [u for u in base if int(combined_status.get(u, 0)) != 200]
        else:
            # Skip anything we have seen in combined (baseline or out)
            work_links = [u for u in unique_links if u not in combined_status or int(combined_status.get(u, 0)) != 200]
    else:
        # Even without resume, skip URLs already 200 in baseline/out
        work_links = [u for u in unique_links if int(combined_status.get(u, 0)) != 200]

    attempts = 0
    completed = 0
    counts: Dict[str, int] = {"200": 0, "404": 0, "500": 0, "timeouts": 0, "other": 0}

    # Progress accounting: pending work only (after resume filtering)
    total_work = len(work_links)
    # Emit minimal progress logs every 10% completed
    percent_checkpoints = []
    if total_work > 0:
        for p in range(10, 100, 10):
            n = int((p / 100.0) * total_work)
            if n > 0:
                percent_checkpoints.append((p, n))
    next_pc_idx = 0

    # Domain-level stats for per-site ETA
    def _domain_of(url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return ""

    total_by_domain: Dict[str, int] = {}
    for u in work_links:
        d = _domain_of(u)
        total_by_domain[d] = total_by_domain.get(d, 0) + 1
    domain_stats: Dict[str, Dict[str, float]] = {
        d: {
            "completed": 0.0,
            "failed": 0.0,
            "sum_elapsed": 0.0,
            "remaining": float(n),
            "first_ts": 0.0,
            "last_ts": 0.0,
        }
        for d, n in total_by_domain.items()
    }

    # Optional metrics CSV sampling
    metrics_writer: Optional[csv.writer] = None
    metrics_fh = None
    if metrics_out:
        metrics_out.parent.mkdir(parents=True, exist_ok=True)
        metrics_fh = open(metrics_out, "w", newline="", encoding="utf-8")
        metrics_writer = csv.writer(metrics_fh)
        metrics_writer.writerow([
            "t_epoch",
            "completed",
            "failed",
            "fail_pct",
            "speed_ps",
            "eta_s",
            "remaining",
        ])

    # Async crawl
    results: Dict[str, Dict[str, object]] = {}
    def _http2_available(force_http1: bool = False) -> bool:
        if force_http1:
            return False
        try:
            import h2  # type: ignore
            return True
        except Exception:
            return False

    # Mapping helpers for collection-level controls
    url_to_slug: Dict[str, str] = {}
    remaining_by_slug: Dict[str, set] = {}
    try:
        url_to_slug = (
            df.set_index(df["external_link"].astype(str))["collection_slug"].to_dict()
            if not df.empty
            else {}
        )
        for slug, grp in df.groupby("collection_slug"):
            remaining_by_slug[str(slug)] = set(grp["external_link"].astype(str).tolist())
    except Exception:
        url_to_slug = {}
        remaining_by_slug = {}

    timeouts_streak_by_slug: Dict[str, int] = defaultdict(int)
    skipped_due_to_streak_collections: set = set()
    skipped_due_to_streak_urls: set = set()

    async with httpx.AsyncClient(
        http2=_http2_available(force_http1=getattr(run, "_force_http1", False)),
        timeout=_httpx_timeout(timeout_s),
        limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency),
        follow_redirects=True,
    ) as client:
        semaphore = asyncio.Semaphore(concurrency)

        # Pre-populate results for skipped collections as SKIPPED (-3) so they are written/merged
        if not df_skip.empty:
            for url in df_skip["external_link"].astype(str).dropna().unique().tolist():
                results[url] = {
                    "status_code": -3,
                    "elapsed_s": 0.0,
                    "pdf_links_json": json.dumps([], ensure_ascii=False),
                    "pdf_links_count": 0,
                    "retries": 0,
                }

        ua_mode = getattr(run, "_ua_mode", "fixed")

        async def _fetch_once(url: str, retries_value: int) -> int:
            nonlocal attempts, completed
            attempts += 1
            prev_retry_offset = int(prev_retries_by_url.get(url, 0))
            try:
                sc, elapsed, html = await _fetch_page(
                    client,
                    url,
                    semaphore=semaphore,
                    wait_s=wait_s,
                    ua_mode=ua_mode,
                )
                if sc == 200:
                    counts["200"] += 1
                elif sc == 404:
                    counts["404"] += 1
                elif sc == 500:
                    counts["500"] += 1
                else:
                    if sc >= 0:
                        counts["other"] += 1
                    else:
                        counts["timeouts"] += 1
                pdfs = extract_pdf_links_in_order(html, url) if sc == 200 else []
                results[url] = {
                    "status_code": int(sc),
                    "elapsed_s": float(elapsed),
                    "pdf_links_json": json.dumps(pdfs, ensure_ascii=False),
                    "pdf_links_count": int(len(pdfs)),
                    "retries": int(prev_retry_offset + retries_value),
                }
            except httpx.TimeoutException as e:
                try:
                    log.warning("Timeout fetching %s: %s", url, e)
                except Exception:
                    pass
                counts["timeouts"] += 1
                results[url] = {
                    "status_code": -1,
                    "elapsed_s": 0.0,
                    "pdf_links_json": json.dumps([], ensure_ascii=False),
                    "pdf_links_count": 0,
                    "retries": int(prev_retry_offset + retries_value),
                }
            except httpx.HTTPError as e:
                try:
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        log.exception("HTTPError fetching %s", url)
                    else:
                        log.warning("HTTPError fetching %s: %r", url, e)
                except Exception:
                    pass
                counts["other"] += 1
                results[url] = {
                    "status_code": -2,
                    "elapsed_s": 0.0,
                    "pdf_links_json": json.dumps([], ensure_ascii=False),
                    "pdf_links_count": 0,
                    "retries": int(prev_retry_offset + retries_value),
                }
            except Exception:
                try:
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        log.exception("Unexpected error fetching %s", url)
                    else:
                        log.warning("Unexpected error fetching %s", url)
                except Exception:
                    pass
                counts["other"] += 1
                results[url] = {
                    "status_code": -2,
                    "elapsed_s": 0.0,
                    "pdf_links_json": json.dumps([], ensure_ascii=False),
                    "pdf_links_count": 0,
                    "retries": int(prev_retry_offset + retries_value),
                }
            finally:
                completed += 1
                # Track remaining by collection and consecutive timeouts
                try:
                    if url in remaining_by_slug.get(url_to_slug.get(url, ""), set()):
                        remaining_by_slug[url_to_slug.get(url, "")].discard(url)
                except Exception:
                    pass
                try:
                    slug = url_to_slug.get(url)
                    if slug is not None:
                        if int(results[url]["status_code"]) == -1:
                            timeouts_streak_by_slug[slug] += 1
                        else:
                            timeouts_streak_by_slug[slug] = 0
                        # If threshold reached, skip rest of the collection by marking as timeouts
                        if (
                            timeouts_streak_threshold
                            and timeouts_streak_by_slug[slug] >= int(timeouts_streak_threshold)
                            and slug not in skipped_due_to_streak_collections
                        ):
                            try:
                                log.warning(
                                    "Timeouts streak threshold reached for collection %s (%s). Skipping remaining %s URLs as timeouts.",
                                    slug,
                                    timeouts_streak_by_slug[slug],
                                    len(remaining_by_slug.get(slug, set())),
                                )
                            except Exception:
                                pass
                            leftover = list(remaining_by_slug.get(slug, set()))
                            for u2 in leftover:
                                if u2 in results:
                                    continue
                                results[u2] = {
                                    "status_code": -1,
                                    "elapsed_s": 0.0,
                                    "pdf_links_json": json.dumps([], ensure_ascii=False),
                                    "pdf_links_count": 0,
                                    "retries": 0,
                                }
                                skipped_due_to_streak_urls.add(u2)
                            skipped_due_to_streak_collections.add(slug)
                except Exception:
                    pass
                # Update domain stats
                dom = _domain_of(url)
                st = domain_stats.get(dom)
                now = time.perf_counter()
                if st is not None:
                    if st["first_ts"] == 0.0:
                        st["first_ts"] = now
                    st["last_ts"] = now
                    if int(results[url]["status_code"]) == 200:  # type: ignore[index]
                        st["completed"] += 1.0
                        st["sum_elapsed"] += float(results[url]["elapsed_s"]) + float(wait_s)  # type: ignore[index]
                    else:
                        st["failed"] += 1.0
                    st["remaining"] = max(0.0, st["remaining"] - 1.0)

                # Minimal progress log every 10%
                try:
                    if (
                        total_work > 0
                        and next_pc_idx < len(percent_checkpoints)
                        and completed >= percent_checkpoints[next_pc_idx][1]
                    ):
                        pct = percent_checkpoints[next_pc_idx][0]
                        log.info(
                            "Progress %s%% (%s/%s) | 200=%s 404=%s 500=%s to=%s other=%s",
                            pct,
                            completed,
                            total_work,
                            counts.get("200", 0),
                            counts.get("404", 0),
                            counts.get("500", 0),
                            counts.get("timeouts", 0),
                            counts.get("other", 0),
                        )
                        next_pc_idx += 1
                except Exception:
                    pass

                if progress:
                    # progress with speed and ETA
                    try:
                        elapsed_all = max(1e-6, time.perf_counter() - start_ts)
                        speed = completed / elapsed_all
                        remaining = max(0, total_work - completed)
                        eta_s = (remaining / speed) if speed > 0 else 0.0
                        eta_m = int(eta_s // 60)
                        eta_x = int(eta_s % 60)
                        frac = 0.0 if total_work <= 0 else min(1.0, completed / max(1, total_work))
                        width = 40
                        filled = int(width * frac)
                        bar = f"[{'#'*filled}{'.'*(width-filled)}]"
                        failed = counts["404"] + counts["500"] + counts["timeouts"] + counts["other"]
                        fail_pct = (failed / max(1, attempts)) * 100.0
                        # Domain-specific ETA for current domain
                        dom_eta_str = ""
                        if st is not None:
                            dom_elapsed = max(1e-6, (st["last_ts"] - st["first_ts"]))
                            dom_speed = (st["completed"] / dom_elapsed) if dom_elapsed > 0 else 0.0
                            dom_eta = (st["remaining"] / dom_speed) if dom_speed > 0 else 0.0
                            dom_eta_str = f" | {dom} eta {int(dom_eta//60):02d}:{int(dom_eta%60):02d}"
                        sys.stdout.write(
                            f"\r{bar} {completed}/{total_work} ext | {speed:.1f} p/s | ETA {eta_m:02d}:{eta_x:02d} | fail% {fail_pct:.1f}{dom_eta_str} | (200={counts['200']}, 404={counts['404']}, 500={counts['500']}, to={counts['timeouts']}, other={counts['other']})"
                        )
                        sys.stdout.flush()
                    except Exception:
                        pass
                # Periodic metrics sample to CSV
                if metrics_writer and (completed % max(1, metrics_chunk) == 0):
                    failed_total = counts["404"] + counts["500"] + counts["timeouts"] + counts["other"]
                    fail_pct_row = (failed_total / max(1, attempts)) * 100.0
                    elapsed_all = max(1e-6, time.perf_counter() - start_ts)
                    speed_row = completed / elapsed_all
                    remaining_row = max(0, total_work - completed)
                    eta_s_row = (remaining_row / speed_row) if speed_row > 0 else 0.0
                    try:
                        metrics_writer.writerow([
                            int(time.time()),
                            int(completed),
                            int(failed_total),
                            round(fail_pct_row, 4),
                            round(speed_row, 4),
                            int(eta_s_row),
                            int(remaining_row),
                        ])
                        if metrics_fh:
                            metrics_fh.flush()
                    except Exception:
                        pass
                return int(results[url]["status_code"])  # type: ignore[index]

        async def _task(url: str) -> None:
            # Early-out for dynamically skipped URLs (due to timeouts streak) or statically skipped
            if url in skipped_due_to_streak_urls:
                if url not in results:
                    results[url] = {
                        "status_code": -1,
                        "elapsed_s": 0.0,
                        "pdf_links_json": json.dumps([], ensure_ascii=False),
                        "pdf_links_count": 0,
                        "retries": 0,
                    }
                # Count as completed for progress
                nonlocal completed
                completed += 1
                return
            sc = await _fetch_once(url, 0)
            # Do not retry when 404; also honor global retries setting
            if sc == 200 or sc == 404 or retries <= 0:
                return
            for add in range(1, retries + 1):
                if int(results.get(url, {}).get("status_code", 0)) == 200:
                    break
                await _fetch_once(url, add)

        start_ts = time.perf_counter()
        last_ckpt_ts = start_ts
        next_ckpt_at = completed + max(0, int(checkpoint_every)) if checkpoint_every and checkpoint_every > 0 else None

        def _write_parquet_atomic(path: Path, dfw: pd.DataFrame, compression: Optional[str]) -> None:
            tmp = path.with_suffix(path.suffix + ".tmp")
            dfw.to_parquet(tmp, index=False, compression=compression or "snappy")
            os.replace(tmp, path)

        def _do_checkpoint(reason: str = "periodic") -> None:
            nonlocal last_ckpt_ts
            try:
                attempted_set = set(results.keys())
                # Build records only for attempted rows (plus any skipped rows, already in results)
                recs: List[Dict[str, object]] = []
                # Include rows from df only if attempted
                for _, r in df.iterrows():
                    url = str(r["external_link"])
                    if url not in attempted_set:
                        continue
                    rest = results.get(url, {
                        "status_code": -3,
                        "elapsed_s": 0.0,
                        "pdf_links_json": json.dumps([], ensure_ascii=False),
                        "pdf_links_count": 0,
                        "retries": 0,
                    })
                    recs.append({
                        "collection_slug": r["collection_slug"],
                        "collection_url": r["collection_url"],
                        "page_number": int(r["page_number"]) if pd.notna(r["page_number"]) else 0,
                        "edm_url": r["edm_url"],
                        "external_link": url,
                        "language_code": r["language_code"],
                        "type": r["type"],
                        **rest,
                    })
                # Include all skipped rows
                if not df_skip.empty:
                    for _, r in df_skip.iterrows():
                        url = str(r["external_link"])
                        rest = results.get(url, {
                            "status_code": -1,
                            "elapsed_s": 0.0,
                            "pdf_links_json": json.dumps([], ensure_ascii=False),
                            "pdf_links_count": 0,
                            "retries": 0,
                        })
                        recs.append({
                            "collection_slug": r["collection_slug"],
                            "collection_url": r["collection_url"],
                            "page_number": int(r["page_number"]) if pd.notna(r["page_number"]) else 0,
                            "edm_url": r["edm_url"],
                            "external_link": url,
                            "language_code": r["language_code"],
                            "type": r["type"],
                            **rest,
                        })
                if not recs:
                    return
                out_parquet.parent.mkdir(parents=True, exist_ok=True)
                df_new = pd.DataFrame.from_records(recs)
                key = ["collection_slug", "page_number", "edm_url"]
                if out_parquet.exists():
                    try:
                        old = pd.read_parquet(out_parquet)
                        if not old.empty:
                            merged = pd.concat([old, df_new], ignore_index=True)
                            merged["_ok"] = (merged["status_code"] == 200).astype(int)
                            merged["retries"] = merged.get("retries", 0).fillna(0).astype(int)
                            merged = merged.sort_values(key + ["_ok", "retries"]).drop_duplicates(subset=key, keep="last")
                            merged = merged.drop(columns=["_ok"], errors="ignore")
                            _write_parquet_atomic(out_parquet, merged, compression)
                        else:
                            _write_parquet_atomic(out_parquet, df_new, compression)
                    except Exception:
                        _write_parquet_atomic(out_parquet, df_new, compression)
                else:
                    _write_parquet_atomic(out_parquet, df_new, compression)
                last_ckpt_ts = time.perf_counter()
            except Exception:
                # Best-effort; ignore checkpoint errors to not disrupt crawl
                pass
        # Remove skipped URLs from work list (already populated above)
        skip_urls = set(df_skip["external_link"].astype(str).dropna().unique().tolist()) if not df_skip.empty else set()
        work_links = [u for u in work_links if u not in skip_urls]
        tasks = [asyncio.create_task(_task(u)) for u in work_links]
        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except asyncio.CancelledError:
                pass
            # After each task, consider checkpoint conditions
            try:
                now = time.perf_counter()
                if checkpoint_every and checkpoint_every > 0 and next_ckpt_at is not None and completed >= next_ckpt_at:
                    _do_checkpoint("every_N")
                    next_ckpt_at = completed + int(checkpoint_every)
                elif checkpoint_seconds and checkpoint_seconds > 0.0 and (now - last_ckpt_ts) >= float(checkpoint_seconds):
                    _do_checkpoint("every_Ts")
            except Exception:
                pass

    try:
        if progress:
            sys.stdout.write("\n")
            sys.stdout.flush()
    except Exception:
        pass

    # Merge back to per-row records
    attempted = set(results.keys())
    records = []
    # Base iterator includes fetched rows and always includes skipped rows (pre-populated)
    iter_rows: Iterable[pd.Series]
    if resume and retry_only:
        iter_rows = (r for _, r in df.iterrows() if r["external_link"] in attempted)
    else:
        iter_rows = (r for _, r in df.iterrows())
    # Always append skipped rows (ensures presence in output even when retry_only)
    if not df_skip.empty:
        for _, r in df_skip.iterrows():
            attempted.add(str(r["external_link"]))
        # Concatenate iterators by materializing df_skip into list of Series
        iter_rows = list(iter_rows) + [r for _, r in df_skip.iterrows()]
    for r in iter_rows:
        url = str(r["external_link"])
        rest = results.get(url, {
            "status_code": -3,
            "elapsed_s": 0.0,
            "pdf_links_json": json.dumps([], ensure_ascii=False),
            "pdf_links_count": 0,
            "retries": 0,
        })
        records.append({
            "collection_slug": r["collection_slug"],
            "collection_url": r["collection_url"],
            "page_number": int(r["page_number"]) if pd.notna(r["page_number"]) else 0,
            "edm_url": r["edm_url"],
            "external_link": url,
            "language_code": r["language_code"],
            "type": r["type"],
            **rest,
        })

    out_df = pd.DataFrame.from_records(records)

    # Write/merge parquet: prefer best status per (collection_slug, page_number, edm_url)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    key = ["collection_slug", "page_number", "edm_url"]
    if out_parquet.exists():
        try:
            old = pd.read_parquet(out_parquet)
            if not old.empty:
                merged = pd.concat([old, out_df], ignore_index=True)
                merged["_ok"] = (merged["status_code"] == 200).astype(int)
                merged["retries"] = merged.get("retries", 0).fillna(0).astype(int)
                merged = merged.sort_values(key + ["_ok", "retries"]).drop_duplicates(subset=key, keep="last")
                merged = merged.drop(columns=["_ok"], errors="ignore")
                tmp = out_parquet.with_suffix(out_parquet.suffix + ".tmp")
                merged.to_parquet(tmp, index=False, compression=compression or "snappy")
                os.replace(tmp, out_parquet)
            else:
                tmp = out_parquet.with_suffix(out_parquet.suffix + ".tmp")
                out_df.to_parquet(tmp, index=False, compression=compression or "snappy")
                os.replace(tmp, out_parquet)
        except Exception:
            tmp = out_parquet.with_suffix(out_parquet.suffix + ".tmp")
            out_df.to_parquet(tmp, index=False, compression=compression or "snappy")
            os.replace(tmp, out_parquet)
    else:
        if len(out_df) > 0:
            tmp = out_parquet.with_suffix(out_parquet.suffix + ".tmp")
            out_df.to_parquet(tmp, index=False, compression=compression or "snappy")
            os.replace(tmp, out_parquet)
    if metrics_fh:
        try:
            metrics_fh.close()
        except Exception:
            pass


def main() -> None:
    p = argparse.ArgumentParser(description="Crawl external item pages and extract in-order PDF links")
    p.add_argument("--edm", required=True, help="Path to edm_metadata_labeled.parquet")
    p.add_argument("--collections", required=True, help="Path to collections_links_enriched_ge1k_sorted.parquet")
    p.add_argument("--out", required=True, help="Output parquet path")
    p.add_argument("--limit", type=int, default=0, help="Optional max input rows after filtering")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    p.add_argument("--wait", type=float, default=DEFAULT_WAIT_S)
    p.add_argument("--progress", action="store_true")
    p.add_argument("--compression", default="snappy", help="Parquet compression (snappy|zstd|gzip|none)")
    p.add_argument("--retries", type=int, default=0, help="Additional retry attempts per URL (early-stop on 200)")
    p.add_argument("--resume", action="store_true", default=False, help="Resume from existing output parquet state")
    p.add_argument("--retry-only", action="store_true", default=False, help="In resume+retries mode, process only previously non-200 URLs")
    p.add_argument("--metrics-chunk", type=int, default=100, help="Sample interval (completed requests) for metrics CSV")
    p.add_argument("--metrics-out", default=None, help="Optional CSV path for global metrics samples")
    p.add_argument(
        "--skip-collections",
        nargs="*",
        default=[],
        help="One or more collection slugs to skip; marked as timeouts and excluded from fetching",
    )
    p.add_argument("--checkpoint-every", type=int, default=0, help="Write a checkpoint (merge to output) every N completed requests (0 disables)")
    p.add_argument("--checkpoint-seconds", type=float, default=0.0, help="Write a checkpoint at least every T seconds (0 disables)")
    p.add_argument(
        "--timeouts-streak-threshold",
        type=int,
        default=0,
        help="If >0, when a collection accumulates this many consecutive timeouts, skip its remaining URLs and mark them as timeouts",
    )
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging verbosity")
    p.add_argument("--force-http1", action="store_true", help="Disable HTTP/2 and use HTTP/1.1 for requests")
    p.add_argument("--random-user-agent", action="store_true", help="Use randomized User-Agent per request")
    p.add_argument("--per-domain-user-agent", action="store_true", help="Use a deterministic random User-Agent per domain")
    p.add_argument("--resume-baseline", default=None, help="Optional master parquet path; skip URLs already 200 there")
    p.add_argument("--only-collections", nargs="*", default=None, help="If provided, include only these collection slugs")
    args = p.parse_args()

    # Configure logging according to CLI
    level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")
    # Keep httpx/httpcore quiet unless DEBUG requested
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    if level <= logging.DEBUG:
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)

    comp = None if (args.compression or "snappy").lower() == "none" else args.compression
    # Pass force-http1 flag to run() via attribute to avoid signature change
    setattr(run, "_force_http1", bool(args.force_http1))
    setattr(run, "_ua_mode", "random" if args.random_user_agent else ("per_domain" if args.per_domain_user_agent else "fixed"))
    asyncio.run(
        run(
            Path(args.edm),
            Path(args.collections),
            Path(args.out),
            int(args.limit),
            int(args.concurrency),
            float(args.timeout),
            float(args.wait),
            bool(args.progress),
            comp,
            int(args.retries),
            bool(args.resume),
            bool(args.retry_only),
            int(args.metrics_chunk),
            Path(args.metrics_out) if args.metrics_out else None,
            skip_collections=[s for s in (args.skip_collections or []) if s],
            checkpoint_every=int(args.checkpoint_every),
            checkpoint_seconds=float(args.checkpoint_seconds),
            timeouts_streak_threshold=int(args.timeouts_streak_threshold),
            baseline_parquet=Path(args.resume_baseline) if args.resume_baseline else None,
            only_collections=[s for s in (args.only_collections or []) if s],
        )
    )


if __name__ == "__main__":
    main()
