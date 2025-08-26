#!/usr/bin/env python3
"""Single-EDM quick test with clear logging.

Steps:
- GET collection page 1 via /search?page.page=1
- Extract first EDM URL
- GET EDM page
- Extract provider link using robust selector
- Save HTML snapshots
- Print each step's status and timing
"""

import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

COLLECTION_URL = "https://www.openarchives.gr/aggregator-openarchives/portal/collections/hnps"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) "
        "Gecko/20100101 Firefox/141.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
SNAP = Path("snapshots/openarchives/quick")
SNAP.mkdir(parents=True, exist_ok=True)


def extract_document_links(listing_html: str, base: str):
    soup = BeautifulSoup(listing_html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/aggregator-openarchives/edm/" in href:
            absu = urljoin(base, href)
            if absu not in links:
                links.append(absu)
    return links


def extract_provider_link(html: str):
    soup = BeautifulSoup(html, "html.parser")
    a = soup.select_one("a.right-column-title-link[href]")
    if a and a.get("href"):
        return a["href"].strip()
    sidebar = soup.select_one("#containerId .col-md-4")
    if sidebar:
        for link in sidebar.find_all("a", href=True):
            href = link["href"].strip()
            text = link.get_text(" ", strip=True).lower()
            if not href:
                continue
            if "openarchives.gr" in href:
                continue
            if any(t in text for t in ("rdf", "json-ld", "jsonld")):
                continue
            return href
    return None


def main():
    s = requests.Session()
    s.headers.update(HEADERS)

    page1 = f"{COLLECTION_URL}/search?page.page=1&sortResults=SCORE"
    t0 = time.perf_counter()
    r = s.get(page1, timeout=(3, 10))
    t1 = time.perf_counter()
    print(f"LISTING GET {page1} -> {r.status_code} in {(t1 - t0):.3f}s")
    (SNAP / "listing_p1.html").write_text(r.text, encoding="utf-8")

    doc_links = extract_document_links(r.text, page1)
    if not doc_links:
        print("No EDM links found on page 1")
        return
    first = doc_links[0]
    print(f"First EDM: {first}")

    t2 = time.perf_counter()
    re = s.get(first, timeout=(3, 10))
    t3 = time.perf_counter()
    print(f"EDM GET -> {re.status_code} in {(t3 - t2):.3f}s")
    name = first.rstrip("/").split("/")[-1]
    (SNAP / f"edm_{name}.html").write_text(re.text, encoding="utf-8")

    provider = extract_provider_link(re.text)
    print(f"Provider link: {provider}")


if __name__ == "__main__":
    main()

