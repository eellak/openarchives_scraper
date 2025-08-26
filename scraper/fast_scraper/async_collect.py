"""Asynchronous collector for HNPS collection links with concurrency 5.

Fetches all pages from /collections/hnps/search?page.page=N, extracts EDM
document links, records HTTP status/elapsed, stores links+page to Parquet,
and reports duplicate pages and duplicate links across pages.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import httpx
import pandas as pd

from .parsers import (
    build_search_page_url,
    extract_document_links,
    parse_total_pages,
)


COLLECTION_URL = "https://www.openarchives.gr/aggregator-openarchives/portal/collections/hnps"
OUTPUT_PARQUET = Path("openarchives_data/hnps_links.parquet")
ERROR_DIR = Path("snapshots/openarchives/errors")
TIMEOUT_SECONDS = 3.0


@dataclass
class PageResult:
    page_number: int
    page_url: str
    status_code: int
    elapsed_s: float
    links: List[str]
    error_html_path: str | None = None


def _httpx_timeout(seconds: float = TIMEOUT_SECONDS) -> httpx.Timeout:
    return httpx.Timeout(connect=seconds, read=seconds, write=seconds, pool=seconds)


async def _fetch_page(
    client: httpx.AsyncClient,
    page_number: int,
    semaphore: asyncio.Semaphore,
) -> PageResult:
    page_url = build_search_page_url(COLLECTION_URL, page_number)
    async with semaphore:
        t0 = time.perf_counter()
        logging.info("[page %s] fetch start → %s", page_number, page_url)
        try:
            resp = await client.get(page_url)
            elapsed = time.perf_counter() - t0
            html = resp.text
            links = extract_document_links(html, page_url)

            error_path: str | None = None
            if resp.status_code != 200:
                ERROR_DIR.mkdir(parents=True, exist_ok=True)
                path = ERROR_DIR / f"hnps_search_page_{page_number}.html"
                path.write_text(html, encoding="utf-8")
                error_path = str(path)

            logging.info(
                "[page %s] status=%s elapsed=%.3fs links=%s",
                page_number,
                resp.status_code,
                elapsed,
                len(links),
            )
            return PageResult(
                page_number=page_number,
                page_url=page_url,
                status_code=resp.status_code,
                elapsed_s=elapsed,
                links=links,
                error_html_path=error_path,
            )
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            ERROR_DIR.mkdir(parents=True, exist_ok=True)
            path = ERROR_DIR / f"hnps_search_page_{page_number}_exception.txt"
            path.write_text(str(exc), encoding="utf-8")
            logging.error("[page %s] exception after %.3fs: %s", page_number, elapsed, exc)
            return PageResult(
                page_number=page_number,
                page_url=page_url,
                status_code=0,
                elapsed_s=elapsed,
                links=[],
                error_html_path=str(path),
            )


async def collect_all(concurrency: int = 5) -> Tuple[List[PageResult], int]:
    # First request to find total pages
    first_url = build_search_page_url(COLLECTION_URL, 1)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(http2=True, timeout=_httpx_timeout(3.0), limits=limits, headers={
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) "
            "Gecko/20100101 Firefox/141.0"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }) as client:
        logging.info("Probe first page to detect total pages → %s", first_url)
        resp = await client.get(first_url)
        html = resp.text
        total_pages = parse_total_pages(html) or 1
        logging.info("Detected total pages: %s", total_pages)

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [asyncio.create_task(_fetch_page(client, page_num, semaphore)) for page_num in range(1, total_pages + 1)]
        results: List[PageResult] = []
        completed = 0
        for coro in asyncio.as_completed(tasks):
            res = await coro
            results.append(res)
            completed += 1
            if completed % 1 == 0:
                logging.info("Progress: %s/%s pages (%.1f%%)", completed, total_pages, completed * 100.0 / total_pages)
        return results, total_pages


def write_parquet(results: List[PageResult], out_path: Path = OUTPUT_PARQUET) -> None:
    records = []
    for r in results:
        for link in r.links:
            records.append({
                "page_number": r.page_number,
                "page_url": r.page_url,
                "status_code": r.status_code,
                "elapsed_s": r.elapsed_s,
                "edm_url": link,
                "error_html_path": r.error_html_path,
            })
    df = pd.DataFrame.from_records(records)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    logging.info("Wrote %s rows to %s", len(df), out_path)


def analyze_duplicates(results: List[PageResult]) -> None:
    # Identical pages: compare sets of links
    set_map: Dict[frozenset, List[int]] = {}
    for r in results:
        key = frozenset(r.links)
        set_map.setdefault(key, []).append(r.page_number)

    dup_pages = [pages for pages in set_map.values() if len(pages) > 1]
    if dup_pages:
        logging.warning("Identical page link-sets detected:")
        for group in sorted(dup_pages):
            logging.warning("  Pages: %s", group)
    else:
        logging.info("No identical page link-sets detected.")

    # Duplicate links across pages
    link_to_pages: Dict[str, List[int]] = {}
    for r in results:
        for link in r.links:
            link_to_pages.setdefault(link, []).append(r.page_number)
    dup_links = {k: v for k, v in link_to_pages.items() if len(set(v)) > 1}
    logging.info("Duplicate links across pages: %s", len(dup_links))
    if dup_links:
        sample = list(dup_links.items())[:10]
        for url, pages in sample:
            logging.warning("  %s → pages %s", url, sorted(set(pages)))


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.info("Starting async collection | concurrency=%s | timeout=%.1fs", 5, TIMEOUT_SECONDS)
    t0 = time.time()
    results, total_pages = asyncio.run(collect_all(concurrency=5))
    write_parquet(results)
    analyze_duplicates(results)
    logging.info("Total runtime: %.3fs for %s pages", time.time() - t0, total_pages)


if __name__ == "__main__":
    main()


