#!/usr/bin/env python3
"""Collect all collections across institutions with names and item counts.

Plan alignment:
- Iterate institutions listing pages (default 4 pages)
- For each institution, fetch its page and extract all collections
- For each collection, capture name and displayed item count ("τεκμήρια")
- Save HTML snapshots at each level for inspection
- Write a parquet with institutions+collections metadata
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


INSTITUTIONS_URL = (
    "https://www.openarchives.gr/aggregator-openarchives/portal/institutions"
)

SNAP_ROOT = Path("snapshots/openarchives")
SNAP_INST_LIST = SNAP_ROOT / "institutions"
SNAP_COLL_INDEX = SNAP_ROOT / "collections_index"
SNAP_COLL_DETAIL = SNAP_ROOT / "collections_detail"
SNAP_INST_LIST.mkdir(parents=True, exist_ok=True)
SNAP_COLL_INDEX.mkdir(parents=True, exist_ok=True)
SNAP_COLL_DETAIL.mkdir(parents=True, exist_ok=True)

OUTPUT_PARQUET = Path("openarchives_data/collections.parquet")
TIMEOUT_SECONDS = 3.0


@dataclass
class CollectionRow:
    institution_slug: str
    institution_name: str
    institution_url: str
    collection_slug: str
    collection_name: str
    collection_url: str
    item_count: Optional[int]
    # provenance
    institution_name_selector: Optional[str] = None
    collection_selector: Optional[str] = None
    count_selector: Optional[str] = None
    snapshot_path: Optional[str] = None


def _httpx_timeout(seconds: float = TIMEOUT_SECONDS) -> httpx.Timeout:
    return httpx.Timeout(connect=seconds, read=seconds, write=seconds, pool=seconds)


def _make_client(concurrency: int) -> httpx.AsyncClient:
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
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
    )


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]


def _save_snapshot(path: Path, html: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")


def _parse_institution_links(html: str, base_url: str) -> List[Tuple[str, str]]:
    """Return list of (institution_url, institution_name). Deduplicated, in order.

    Heuristic: anchors with href containing /portal/institutions/.
    Name: anchor text if non-empty; else fallback to slug.
    """
    soup = BeautifulSoup(html, "html.parser")
    items: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/aggregator-openarchives/portal/institutions/" in href:
            url = urljoin(base_url, href)
            slug = _slug_from_url(url)
            if url in seen:
                continue
            seen.add(url)
            name = a.get_text(" ", strip=True) or slug
            items.append((url, name))
    return items


def _parse_count_from_text(text: str) -> Optional[int]:
    m = re.search(r"([\d\.,]+)\s+τεκμήρια", text)
    if m:
        num = m.group(1).replace(".", "").replace(",", "")
        try:
            return int(num)
        except ValueError:
            return None
    return None


def _parse_collections_from_institution(html: str, base_url: str) -> List[Tuple[str, str, Optional[int], str, str]]:
    """Return list of (collection_url, collection_name, item_count, collection_selector, count_selector).

    Rules:
    - Only accept anchors matching a.collection_title
    - Name from anchor text
    - Item count from nearest sibling/ancestor block's div.collection_type .lastText text
    """
    soup = BeautifulSoup(html, "html.parser")
    results: List[Tuple[str, str, Optional[int], str, str]] = []
    seen_urls: set[str] = set()

    for a in soup.select("a.collection_title[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        url = urljoin(base_url, href)
        if url in seen_urls:
            continue
        seen_urls.add(url)

        name = a.get_text(" ", strip=True) or _slug_from_url(url)

        # Find count within the same visual block
        item_count: Optional[int] = None
        block = a
        # Walk up to four ancestor levels looking for collection_type
        for _ in range(4):
            if block is None:
                break
            container = getattr(block, "find", lambda *args, **kwargs: None)("div", class_="collection_type")
            if container:
                last = container.select_one(".lastText")
                text = last.get_text(" ", strip=True) if last else container.get_text(" ", strip=True)
                item_count = _parse_count_from_text(text)
                if item_count is not None:
                    break
            block = block.parent

        results.append((url, name, item_count, "a.collection_title[href]", "div.collection_type .lastText|container_text"))

    return results


async def _fetch_text(client: httpx.AsyncClient, url: str) -> Tuple[int, str]:
    resp = await client.get(url)
    return resp.status_code, resp.text


async def _collect_institutions(client: httpx.AsyncClient, max_pages: int) -> List[Tuple[str, str]]:
    institutions: List[Tuple[str, str]] = []
    for page in range(1, max_pages + 1):
        page_url = f"{INSTITUTIONS_URL}?page.page={page}&query="
        logging.info("[inst list] Fetch p%s → %s", page, page_url)
        status, html = await _fetch_text(client, page_url)
        if status != 200:
            logging.warning("[inst list] Non-200 status=%s for page %s; stopping", status, page)
            break
        # Snapshot
        _save_snapshot(SNAP_INST_LIST / f"listing_p{page}.html", html)
        items = _parse_institution_links(html, page_url)
        logging.info("[inst list] p%s found %s institutions", page, len(items))
        if not items:
            break
        institutions.extend(items)
    # Deduplicate while preserving order
    seen = set()
    unique: List[Tuple[str, str]] = []
    for url, name in institutions:
        if url not in seen:
            seen.add(url)
            unique.append((url, name))
    return unique


async def _collect_collections_for_institution(
    client: httpx.AsyncClient,
    inst_url: str,
    inst_name_hint: str,
) -> List[CollectionRow]:
    slug = _slug_from_url(inst_url)
    snap_path = SNAP_INST_LIST / f"inst_{slug}.html"

    # Read from snapshot if present; otherwise fetch and save
    if snap_path.exists():
        html = snap_path.read_text(encoding="utf-8")
        status = 200
    else:
        status, html = await _fetch_text(client, inst_url)
        if status != 200:
            logging.warning("[inst] %s -> status=%s", inst_url, status)
            return []
        _save_snapshot(snap_path, html)

    # Institution name: prefer <h1> <a.organisation_title>, then <h1>, then <title>
    soup = BeautifulSoup(html, "html.parser")
    inst_name = inst_name_hint
    inst_sel_used = None
    a_org = soup.select_one("h1 a.organisation_title") or soup.select_one("a.organisation_title")
    if a_org and a_org.get_text(strip=True):
        inst_name = a_org.get_text(" ", strip=True)
        inst_sel_used = "h1 a.organisation_title|a.organisation_title"
    else:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            inst_name = h1.get_text(" ", strip=True)
            inst_sel_used = "h1"
        else:
            t = soup.find("title")
            if t and t.get_text(strip=True):
                # Titles are like "OpenArchives.gr | <Name>" → take the part after '|'
                title_text = t.get_text(strip=True)
                parts = [p.strip() for p in title_text.split("|")]
                inst_name = parts[-1] if parts else inst_name_hint
                inst_sel_used = "title|split('|')"

    tuples = _parse_collections_from_institution(html, inst_url)
    rows: List[CollectionRow] = []
    for coll_url, coll_name, item_count, coll_sel, count_sel in tuples:
        rows.append(
            CollectionRow(
                institution_slug=slug,
                institution_name=inst_name,
                institution_url=inst_url,
                collection_slug=_slug_from_url(coll_url),
                collection_name=coll_name,
                collection_url=coll_url,
                item_count=item_count,
                institution_name_selector=inst_sel_used,
                collection_selector=coll_sel,
                count_selector=count_sel,
                snapshot_path=str(snap_path),
            )
        )
    logging.info("[inst] %s collections=%s", inst_url, len(rows))
    return rows


async def run(max_pages: int, concurrency: int) -> List[CollectionRow]:
    async with _make_client(concurrency) as client:
        institutions = await _collect_institutions(client, max_pages)
        logging.info("Total institutions discovered: %s", len(institutions))

        sem = asyncio.Semaphore(concurrency)
        async def _task(inst: Tuple[str, str]) -> List[CollectionRow]:
            async with sem:
                return await _collect_collections_for_institution(client, inst[0], inst[1])

        tasks = [asyncio.create_task(_task(inst)) for inst in institutions]
        results: List[CollectionRow] = []
        completed = 0
        for coro in asyncio.as_completed(tasks):
            rows = await coro
            results.extend(rows)
            completed += 1
            if completed % 5 == 0 or completed == len(tasks):
                logging.info("Institutions processed: %s/%s (%.1f%%)", completed, len(tasks), completed * 100.0 / max(1, len(tasks)))
        
        # Fallback: merge in any collections visible on the global collections listing but missing from institution pages
        present_urls = {r.collection_url for r in results}
        index_urls: List[str] = []
        for page in range(1, max_pages + 1):
            list_url = f"https://www.openarchives.gr/aggregator-openarchives/portal/collections?page.page={page}&query="
            status, html = await _fetch_text(client, list_url)
            if status != 200:
                logging.warning("[coll index] non-200 status=%s for page %s", status, page)
                break
            _save_snapshot(SNAP_COLL_INDEX / f"collections_p{page}.html", html)
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.select("a.collection_title[href]"):
                href = a.get("href", "").strip()
                if "/aggregator-openarchives/portal/collections/" in href:
                    index_urls.append(urljoin(list_url, href))
        index_urls = list(dict.fromkeys(index_urls))
        missing_urls = [u for u in index_urls if u not in present_urls]
        if missing_urls:
            logging.info("[coll index] Adding missing collections from index: %s", len(missing_urls))
            sem = asyncio.Semaphore(concurrency)

            async def _fetch_detail(url: str) -> Optional[CollectionRow]:
                async with sem:
                    try:
                        status, html = await _fetch_text(client, url)
                        if status != 200:
                            return None
                        # snapshot
                        _save_snapshot(SNAP_COLL_DETAIL / f"collection_{_slug_from_url(url)}.html", html)
                        soup = BeautifulSoup(html, "html.parser")
                        # institution link present on collection page
                        inst_anchor = None
                        for a in soup.find_all("a", href=True):
                            if "/aggregator-openarchives/portal/institutions/" in a.get("href", ""):
                                inst_anchor = a
                                break
                        if not inst_anchor:
                            return None
                        inst_url = urljoin(url, inst_anchor.get("href", ""))
                        inst_name = inst_anchor.get_text(" ", strip=True) or _slug_from_url(inst_url)
                        # collection name from h1
                        h1 = soup.find("h1")
                        coll_name = h1.get_text(" ", strip=True) if h1 else _slug_from_url(url)
                        # item count heuristic on detail page
                        count = _parse_count_from_text(soup.get_text(" ", strip=True))
                        return CollectionRow(
                            institution_slug=_slug_from_url(inst_url),
                            institution_name=inst_name,
                            institution_url=inst_url,
                            collection_slug=_slug_from_url(url),
                            collection_name=coll_name,
                            collection_url=url,
                            item_count=count,
                        )
                    except Exception:
                        return None

            detail_tasks = [asyncio.create_task(_fetch_detail(u)) for u in missing_urls]
            added = 0
            for coro in asyncio.as_completed(detail_tasks):
                row = await coro
                if row is not None:
                    results.append(row)
                    added += 1
            logging.info("[coll index] Added %s collections from index fallback", added)
        
        return results


def _rows_to_parquet(rows: List[CollectionRow], out_path: Path = OUTPUT_PARQUET) -> None:
    records = [
        {
            "institution_slug": r.institution_slug,
            "institution_name": r.institution_name,
            "institution_url": r.institution_url,
            "collection_slug": r.collection_slug,
            "collection_name": r.collection_name,
            "collection_url": r.collection_url,
            "item_count": r.item_count,
        }
        for r in rows
    ]
    df = pd.DataFrame.from_records(records)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    logging.info("Wrote %s collections to %s", len(df), out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect OpenArchives collections metadata across institutions")
    parser.add_argument("--max-pages", type=int, default=4, help="Max institutions listing pages to scan (default 4)")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent institutions fetches")
    parser.add_argument("--out", type=str, default=str(OUTPUT_PARQUET), help="Output parquet path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    t0 = time.time()
    rows = asyncio.run(run(args.max_pages, args.concurrency))
    _rows_to_parquet(rows, Path(args.out))
    logging.info("Done in %.3fs", time.time() - t0)


if __name__ == "__main__":
    main()


