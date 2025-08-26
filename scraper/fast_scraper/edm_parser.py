"""High-performance EDM document parser for OpenArchives pages.

Design goals:
- Strict selectors with evidence-backed fallbacks only
- No guesses: if a target is missing, return None/empty
- Minimize allocations and parsing overhead

Public API:
- parse_external_link(html: str, base_url: str) -> Optional[str]
- parse_metadata(html: str) -> Dict[str, str]

Implementation notes:
- Uses BeautifulSoup with the built-in html.parser for speed and stability
- Avoids complex CSS selection; uses targeted find/find_all
- Only extracts external link from explicit anchor with class right-column-title-link and http(s) href
- Metadata extracted from container div.form-horizontal.fieldset.container-fluid; groups by form-group
"""

from __future__ import annotations

from typing import Dict, Optional, Any
import asyncio
import time
import httpx
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def parse_external_link(html: str, base_url: str) -> Optional[str]:
    """Extract explicit external link from EDM page.

    Evidence-backed selector: <a class="right-column-title-link" href^="http">
    Returns absolute URL or None if not present.
    """
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    a = soup.find("a", attrs={"class": "right-column-title-link"}, href=True)
    if a is None:
        return None
    href = (a.get("href") or "").strip()
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(base_url, href)


def parse_metadata(html: str) -> Dict[str, str]:
    """Extract key-value metadata from the EDM details panel.

    Evidence-backed container: div.form-horizontal.fieldset.container-fluid
    Within it, iterate div.form-group blocks:
      - key from label.control-label
      - value from div.col-sm-9 > div.item-control-static (fallback to text of div.col-sm-9)
    Only returns pairs with non-empty key and value; preserves first occurrence.
    """
    result: Dict[str, str] = {}
    if not html:
        return result
    soup = BeautifulSoup(html, "html.parser")

    # Locate the container by requiring all three classes to be present
    container = None
    for div in soup.find_all("div", class_=True):
        classes = div.get("class", [])
        if all(cls in classes for cls in ["form-horizontal", "fieldset", "container-fluid"]):
            container = div
            break
    if container is None:
        return result

    # Extract field groups
    for group in container.find_all("div", class_="form-group"):
        label_el = group.find("label", class_="control-label")
        body = group.find("div", class_="col-sm-9")
        if label_el is None or body is None:
            continue
        key = label_el.get_text(" ", strip=True)
        if not key or key in result:
            continue
        value_el = body.find("div", class_="item-control-static")
        value_text = (value_el.get_text(" ", strip=True) if value_el is not None else body.get_text(" ", strip=True))
        value_text = value_text.strip()
        if value_text:
            result[key] = value_text

    return result


async def fetch_and_parse_edm(
    client: httpx.AsyncClient,
    edm_url: str,
    *,
    semaphore: Optional[asyncio.Semaphore] = None,
    wait_s: float = 0.0,
    with_provenance: bool = False,
) -> Dict[str, Any]:
    """Fetch a single EDM page and parse external link and metadata.

    - Uses the strict selectors implemented above
    - Early and cheap: minimal sleeps and allocations
    - Optional provenance to avoid bloating outputs
    """
    async def _fetch() -> httpx.Response:
        if wait_s and wait_s > 0:
            await asyncio.sleep(wait_s)
        return await client.get(edm_url)

    t0 = time.perf_counter()
    if semaphore is None:
        resp = await _fetch()
    else:
        async with semaphore:
            resp = await _fetch()
    elapsed = time.perf_counter() - t0

    html = resp.text if resp.status_code == 200 else ""
    external = parse_external_link(html, edm_url) if html else None
    meta = parse_metadata(html) if html else {}

    out: Dict[str, Any] = {
        "url": edm_url,
        "status_code": int(resp.status_code),
        "elapsed_s": float(elapsed),
        "external_link": external,
        "metadata": meta,
    }
    if with_provenance:
        out["provenance"] = {
            "external_selector": 'a.right-column-title-link[href^="http"]',
            "metadata_container_selector": 'div.form-horizontal.fieldset.container-fluid',
            "num_fields": len(meta),
        }
    return out


