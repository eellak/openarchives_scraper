#!/usr/bin/env python3
"""Async fetcher that reads edm_url from a parquet and fetches pages.

- Logs at request start and on response
- Concurrency control (default 5)
- Optional limit of first N URLs
- Writes results back into the SAME parquet by adding columns:
  - ``edm_status_code``: HTTP status for the EDM page
  - ``external_link``: first non-openarchives absolute URL found in the page (if any)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from pathlib import Path
from typing import List, Tuple, Optional

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

# Configure module logger
logger = logging.getLogger(__name__)
# Ensure basic config if not already set
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

TIMEOUT_SECONDS = 3.0


def _httpx_timeout(seconds: float = TIMEOUT_SECONDS) -> httpx.Timeout:
    return httpx.Timeout(connect=seconds, read=seconds, write=seconds, pool=seconds)


def _make_client(concurrency: int) -> httpx.AsyncClient:
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)

    async def _log_request(request: httpx.Request):
        logger.debug("HTTPX request: %s %s", request.method, request.url)

    async def _log_response(response: httpx.Response):
        logger.debug(
            "HTTPX response: %s %s -> %s",
            response.request.method,
            response.request.url,
            response.status_code,
        )

    return httpx.AsyncClient(
        http2=True,
        timeout=_httpx_timeout(),
        limits=limits,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) "
                "Gecko/20100101 Firefox/141.0"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        event_hooks={
            "request": [_log_request],
            "response": [_log_response],
        },
    )


def _extract_external_link(page_html: str, base_url: Optional[str] = None) -> Optional[str]:
    """Return ONLY the explicit original-page anchor if present.

    Rule: return href from a.right-column-title-link[href^="http"].
    No other fallbacks are allowed.
    """
    soup = BeautifulSoup(page_html, "html.parser")
    el = soup.select_one('a.right-column-title-link[href^="http"]')
    if not el:
        return None
    href = el.get("href", "").strip()
    if not href:
        return None
    # Already absolute per selector; keep base_url logic for safety
    if base_url and not href.lower().startswith(("http://", "https://")):
        href = urljoin(base_url, href)
    return href if href.lower().startswith(("http://", "https://")) else None


async def _fetch_one(
    client: httpx.AsyncClient,
    url: str,
    sem: asyncio.Semaphore,
) -> Tuple[str, int, float, Optional[str]]:
    """Fetch a single URL with proper error handling and extract external link.

    Returns:
        Tuple of (url, status_code, elapsed_time, external_link)
        - status_code: HTTP status or 0 for timeout, -1 for other errors
        - external_link: first non-openarchives absolute link in page (if any)
    """
    async with sem:
        t0 = time.perf_counter()
        try:
            logger.info("[fetch] start %s", url)
            resp = await client.get(url)
            elapsed = time.perf_counter() - t0
            ext = _extract_external_link(resp.text, base_url=url) if resp.text else None
            logger.info("[fetch] %s -> status=%s elapsed=%.3fs", url, resp.status_code, elapsed)
            return url, resp.status_code, elapsed, ext
        except httpx.TimeoutException:
            elapsed = time.perf_counter() - t0
            logger.warning("[fetch] timeout for %s after %.3fs", url, elapsed)
            return url, 0, elapsed, None
        except httpx.RequestError as e:
            elapsed = time.perf_counter() - t0
            logger.error("[fetch] request error for %s after %.3fs: %s", url, elapsed, e)
            return url, -1, elapsed, None
        except Exception as e:
            elapsed = time.perf_counter() - t0
            logger.error("[fetch] unexpected error for %s after %.3fs: %s", url, elapsed, e)
            return url, -1, elapsed, None


async def run(parquet_path: Path, limit: int | None, concurrency: int) -> None:
    # Read parquet with error handling
    try:
        df = pd.read_parquet(parquet_path)
    except FileNotFoundError:
        logger.error("Parquet file not found: %s", parquet_path)
        return
    except Exception as e:
        logger.error("Failed to read parquet file %s: %s", parquet_path, e)
        return
    
    if "edm_url" not in df.columns:
        logger.error("Column 'edm_url' not found in parquet file")
        return
    
    # Ensure output columns exist (do NOT overwrite listing-phase status_code)
    if "edm_status_code" not in df.columns:
        df["edm_status_code"] = pd.Series([None] * len(df), dtype="Int64")
    if "external_link" not in df.columns:
        df["external_link"] = pd.Series([None] * len(df), dtype="string")

    # Prepare list of (row_index, absolute_url)
    indices_and_urls: List[Tuple[int, str]] = []
    max_rows = limit if limit is not None else len(df)
    for idx, u in list(df["edm_url"].items())[:max_rows]:
        abs_url = u if isinstance(u, str) and u.startswith("http") else "https://www.openarchives.gr" + str(u)
        indices_and_urls.append((idx, abs_url))
    
    logger.info("Processing %s URLs from %s", len(indices_and_urls), parquet_path)

    async with _make_client(concurrency) as client:
        sem = asyncio.Semaphore(concurrency)
        tasks = [asyncio.create_task(_fetch_one(client, u, sem)) for _, u in indices_and_urls]
        done = 0
        for coro in asyncio.as_completed(tasks):
            url, status, elapsed, ext = await coro
            done += 1
            if done % 10 == 0 or done == len(tasks):
                logger.info("Progress: %s/%s (%.1f%%)", done, len(tasks), done * 100.0 / len(tasks))

            # Update the matching row in the dataframe
            # Find the row index corresponding to this URL among the processed subset
            try:
                idx = next(i for i, u in indices_and_urls if u == url)
            except StopIteration:
                continue
            # Assign results
            try:
                df.at[idx, "edm_status_code"] = int(status) if status is not None else None
            except Exception:
                df.at[idx, "edm_status_code"] = None
            df.at[idx, "external_link"] = ext if ext else "not found"

    # Write results back to the SAME parquet path
    try:
        df.to_parquet(parquet_path, index=False)
        logger.info("Updated parquet written to %s", parquet_path)
    except Exception as e:
        logger.error("Failed to write updated parquet to %s: %s", parquet_path, e)


def main():
    parser = argparse.ArgumentParser(description="Fetch EDM URLs from parquet and enrich the parquet with status/external link")
    parser.add_argument("--parquet", default="openarchives_data/hnps_links.parquet")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=5)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", force=True)
    t0 = time.time()
    asyncio.run(run(Path(args.parquet), args.limit, args.concurrency))
    logging.info("Done in %.3fs", time.time() - t0)


if __name__ == "__main__":
    main()

