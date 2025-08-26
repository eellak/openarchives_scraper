#!/usr/bin/env python3
"""Check which collections are online by probing an external link per collection.

Process:
- Read collections parquet (collection_url, names etc.)
- For each collection:
  - Visit listing page 1 (collection_url + /search?page.page=1)
  - Extract document (EDM) links; fetch each doc page until we find an external link
    - If none on page, paginate to next page; stop once one external link is found or pages exhausted
- After discovery, concurrently probe the discovered external links with high concurrency and timeout
- Write results back into the SAME parquet with columns:
  - probe_external_url
  - probe_status_code
  - probe_elapsed_s
  - probe_note (optional message on failure)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json

import httpx
import pandas as pd
from urllib.parse import urlparse

from .parsers import (
    build_search_page_url,
    extract_document_links,
    parse_total_pages,
)
from .async_fetch_edms_from_parquet import _extract_external_link  # reuse tested logic


DEFAULT_CONCURRENCY_DISCOVERY = 8
DEFAULT_CONCURRENCY_PROBE = 15
DEFAULT_TIMEOUT_S = 15.0

# Snapshots for probe evidence
SNAP_ROOT = Path("snapshots/openarchives")
SNAP_PROBE_DOCS = SNAP_ROOT / "probe_docs"
SNAP_PROBE_DOCS.mkdir(parents=True, exist_ok=True)


def _httpx_timeout(seconds: float) -> httpx.Timeout:
    return httpx.Timeout(connect=seconds, read=seconds, write=seconds, pool=seconds)


def _make_client(concurrency: int, timeout_s: float) -> httpx.AsyncClient:
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async def _log_request(request: httpx.Request):
        logging.info("[http] start %s %s", request.method, request.url)
    async def _log_response(response: httpx.Response):
        logging.info("[http] done %s %s -> %s", response.request.method, response.request.url, response.status_code)
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


async def _fetch_text(client: httpx.AsyncClient, url: str) -> Tuple[int, str]:
    try:
        resp = await client.get(url)
        return resp.status_code, resp.text
    except Exception as exc:
        logging.warning("[fetch_text] error for %s: %s", url, exc)
        return -1, ""


async def _discover_external_for_collection(
    client: httpx.AsyncClient,
    collection_url: str,
    max_pages_cap: Optional[int] = None,
) -> Tuple[Optional[str], str, Optional[str], Optional[str], Optional[str]]:
    """Return (external_url, note, evidence_selector). Note empty on success.

    Iterates listing pages until an external link is found from a document detail page.
    """
    page_num = 1
    total_pages: Optional[int] = None
    visited_docs = 0
    while True:
        list_url = build_search_page_url(collection_url, page_num)
        status, html = await _fetch_text(client, list_url)
        if status != 200 or not html:
            return None, f"listing fetch failed status={status} page={page_num}", None, None, None
        if total_pages is None:
            total_pages = parse_total_pages(html) or 1
        edms = extract_document_links(html, list_url)
        for doc_url in edms:
            visited_docs += 1
            s, doc_html = await _fetch_text(client, doc_url)
            if s != 200 or not doc_html:
                continue
            # Only explicit anchor is allowed by extractor; no other fallbacks
            ext = _extract_external_link(doc_html, base_url=doc_url)
            if ext:
                # Save probe evidence snapshot
                parsed = urlparse(doc_url)
                parts = [p for p in parsed.path.split("/") if p]
                doc_slug = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
                safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in (doc_slug or "doc"))
                snap_path = SNAP_PROBE_DOCS / f"probe_{safe}.html"
                try:
                    snap_path.write_text(doc_html, encoding="utf-8")
                except Exception:
                    snap_path = None
                # Only explicit selector is used in extractor now
                return ext, "", "a.right-column-title-link[href^=\"http\"]", (str(snap_path) if snap_path else None), doc_url
        # next page
        page_num += 1
        if (total_pages and page_num > total_pages) or (max_pages_cap and page_num > max_pages_cap):
            return None, f"not found after {visited_docs} docs in {page_num-1} pages", None, None, None


async def _probe_url(client: httpx.AsyncClient, url: str) -> Tuple[int, float]:
    t0 = time.perf_counter()
    try:
        resp = await client.get(url)
        elapsed = time.perf_counter() - t0
        return resp.status_code, elapsed
    except httpx.TimeoutException:
        return 0, time.perf_counter() - t0
    except Exception:
        return -1, time.perf_counter() - t0


async def run(
    parquet_path: Path,
    discovery_concurrency: int,
    probe_concurrency: int,
    timeout_s: float,
    max_pages_cap: Optional[int],
    retries: int,
    force_discovery: bool,
) -> None:
    # Load dataframe
    df = pd.read_parquet(parquet_path)
    if "collection_url" not in df.columns:
        raise SystemExit("collection_url column not found in parquet")

    # Ensure output columns
    if "probe_external_url" not in df.columns:
        df["probe_external_url"] = pd.Series([None] * len(df), dtype="string")
    if "probe_status_code" not in df.columns:
        df["probe_status_code"] = pd.Series([None] * len(df), dtype="Int64")
    if "probe_elapsed_s" not in df.columns:
        df["probe_elapsed_s"] = pd.Series([None] * len(df), dtype="float64")
    if "probe_note" not in df.columns:
        df["probe_note"] = pd.Series([None] * len(df), dtype="string")
    if "probe_history" not in df.columns:
        df["probe_history"] = pd.Series(["[]"] * len(df), dtype="string")

    # Phase A: discover one external per collection, if not already present
    pending: List[Tuple[int, str]] = []
    for idx, url in df["collection_url"].items():
        val = str(df.at[idx, "probe_external_url"]) if pd.notna(df.at[idx, "probe_external_url"]) else ""
        needs = force_discovery or (not val.strip()) or (val.strip().lower() == "not found")
        if needs:
            pending.append((idx, str(url)))

    logging.info("Collections needing discovery: %s", len(pending))

    async with _make_client(discovery_concurrency, timeout_s) as client:
        sem = asyncio.Semaphore(discovery_concurrency)
        async def _task(index_url: Tuple[int, str]):
            idx, url = index_url
            async with sem:
                ext, note, evidence_sel, doc_snap, doc_url = await _discover_external_for_collection(client, url, max_pages_cap=max_pages_cap)
                df.at[idx, "probe_external_url"] = ext if ext else "not found"
                df.at[idx, "probe_note"] = note if note else None
                if "probe_selector" not in df.columns:
                    df["probe_selector"] = pd.Series([None] * len(df), dtype="string")
                df.at[idx, "probe_selector"] = evidence_sel if ext else None
                if "probe_doc_snapshot" not in df.columns:
                    df["probe_doc_snapshot"] = pd.Series([None] * len(df), dtype="string")
                if "probe_doc_url" not in df.columns:
                    df["probe_doc_url"] = pd.Series([None] * len(df), dtype="string")
                df.at[idx, "probe_doc_snapshot"] = doc_snap if ext else None
                df.at[idx, "probe_doc_url"] = doc_url if ext else None
        tasks = [asyncio.create_task(_task(t)) for t in pending]
        completed = 0
        for coro in asyncio.as_completed(tasks):
            await coro
            completed += 1
            if completed % 10 == 0 or completed == len(tasks):
                logging.info("Discovery progress: %s/%s", completed, len(tasks))

    # Phase B: probe external links concurrently
    to_probe: List[Tuple[int, str]] = []
    for idx, ext in df["probe_external_url"].items():
        if isinstance(ext, str) and ext.strip():
            to_probe.append((idx, ext.strip()))

    logging.info("Probing external links: %s", len(to_probe))

    async def _append_history(idx: int, status: int, elapsed: float) -> None:
        try:
            raw = df.at[idx, "probe_history"]
            hist = json.loads(raw) if isinstance(raw, str) and raw.strip() else []
            hist.append([int(status), float(elapsed)])
            df.at[idx, "probe_history"] = json.dumps(hist, ensure_ascii=False)
        except Exception:
            # best-effort fallback
            df.at[idx, "probe_history"] = json.dumps([[int(status), float(elapsed)]], ensure_ascii=False)

    async def _probe_batch(pairs: List[Tuple[int, str]]) -> None:
        async with _make_client(probe_concurrency, timeout_s) as client:
            sem = asyncio.Semaphore(probe_concurrency)
            async def _probe_task(index_url: Tuple[int, str]):
                idx, url = index_url
                async with sem:
                    status, elapsed = await _probe_url(client, url)
                    await _append_history(idx, status, elapsed)
                    prev_status = df.at[idx, "probe_status_code"]
                    prev_elapsed = df.at[idx, "probe_elapsed_s"]
                    # Overwrite if different or missing
                    if pd.isna(prev_status) or int(prev_status) != int(status) or (
                        pd.notna(prev_elapsed) and float(prev_elapsed) != float(elapsed)
                    ):
                        df.at[idx, "probe_status_code"] = int(status)
                        df.at[idx, "probe_elapsed_s"] = float(elapsed)

            tasks = [asyncio.create_task(_probe_task(t)) for t in pairs]
            completed = 0
            for coro in asyncio.as_completed(tasks):
                await coro
                completed += 1
                if completed % 20 == 0 or completed == len(tasks):
                    logging.info("Probe progress: %s/%s", completed, len(tasks))

    # Initial probe
    await _probe_batch(to_probe)

    # Retries for non-200 statuses
    for attempt in range(1, max(0, retries) + 1):
        retry_pairs: List[Tuple[int, str]] = []
        for idx, ext in df["probe_external_url"].items():
            if not (isinstance(ext, str) and ext.strip()):
                continue
            status = df.at[idx, "probe_status_code"]
            try:
                ok = int(status) == 200
            except Exception:
                ok = False
            if not ok:
                retry_pairs.append((idx, ext.strip()))
        if not retry_pairs:
            break
        logging.info("Retry attempt %s: %s URLs", attempt, len(retry_pairs))
        await _probe_batch(retry_pairs)

    # Write back to same parquet
    df.to_parquet(parquet_path, index=False)
    logging.info("Updated parquet written to %s", parquet_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check collections online status by probing one external link per collection")
    parser.add_argument("--parquet", default="openarchives_data/collections.parquet")
    parser.add_argument("--discover-concurrency", type=int, default=DEFAULT_CONCURRENCY_DISCOVERY)
    parser.add_argument("--probe-concurrency", type=int, default=DEFAULT_CONCURRENCY_PROBE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--max-pages-cap", type=int, default=None, help="Optional cap on pages to scan per collection during discovery")
    parser.add_argument("--retries", type=int, default=1, help="Retry count for non-200 probe results")
    parser.add_argument("--force-discovery", action="store_true", help="Re-run discovery for all collections, overwriting probe_external_url")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    t0 = time.time()
    asyncio.run(run(Path(args.parquet), args.discover_concurrency, args.probe_concurrency, args.timeout, args.max_pages_cap, args.retries, args.force_discovery))
    logging.info("Done in %.3fs", time.time() - t0)


if __name__ == "__main__":
    main()


