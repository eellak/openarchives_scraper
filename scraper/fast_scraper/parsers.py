"""HTML parsers for OpenArchives pages (requests + BeautifulSoup)."""

from typing import List, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
import math

from bs4 import BeautifulSoup

PDF_HINTS = (".pdf", "/bitstreams/")


INSTITUTIONS_URL = "https://www.openarchives.gr/aggregator-openarchives/portal/institutions"


def find_first_collection_url(institutions_html: str, base_url: str = INSTITUTIONS_URL) -> Optional[str]:
    """Return the first collection URL from the institutions page, if any."""
    soup = BeautifulSoup(institutions_html, "html.parser")
    # Try to also capture the first institution name alongside the collection link
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/aggregator-openarchives/portal/collections/" in href:
            return urljoin(base_url, href)
    return None


def find_first_institution_url(institutions_html: str, base_url: str = INSTITUTIONS_URL) -> Optional[str]:
    """Return the first institution URL from the institutions page, if any."""
    soup = BeautifulSoup(institutions_html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/aggregator-openarchives/portal/institutions/" in href:
            return urljoin(base_url, href)
    return None


def find_first_collection_from_institution(inst_html: str, inst_base: str) -> Optional[str]:
    """On an institution page, return the first collection URL."""
    soup = BeautifulSoup(inst_html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/aggregator-openarchives/portal/collections/" in href:
            return urljoin(inst_base, href)
    return None


def extract_document_links(listing_html: str, page_base: str) -> List[str]:
    """Extract EDM document links from a listing page.

    Looks for anchors whose href contains "/aggregator-openarchives/edm/".
    Deduplicates while preserving order.
    """
    soup = BeautifulSoup(listing_html, "html.parser")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/aggregator-openarchives/edm/" in href:
            absolute = urljoin(page_base, href)
            if absolute not in links:
                links.append(absolute)
    return links


def build_search_page_url(collection_url: str, page_number: int) -> str:
    """Return /search URL with page.page=N (and stable sort) for a collection."""
    parsed = urlparse(collection_url)
    # Ensure path ends with /search
    path = parsed.path
    if not path.endswith("/search"):
        if path.endswith('/'):
            path = path + 'search'
        else:
            path = path + '/search'
    qs = parse_qs(parsed.query)
    qs["page.page"] = [str(page_number)]
    qs.setdefault("sortResults", ["SCORE"])  # match observed requests
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, new_query, parsed.fragment))


def find_next_page_url(listing_html: str, page_base: str) -> Optional[str]:
    """Increment page.page for the /search endpoint to get the next page."""
    parsed = urlparse(page_base)
    qs = parse_qs(parsed.query)
    key = "page.page"
    try:
        current_page = int(qs.get(key, ["1"])[0])
    except ValueError:
        current_page = 1
    return build_search_page_url(urlunparse((parsed.scheme, parsed.netloc, parsed.path.rsplit('/search',1)[0], parsed.params, '', parsed.fragment)), current_page + 1)


def parse_collection_name(listing_html: str) -> Optional[str]:
    """Attempt to extract the collection name from the listing page header."""
    soup = BeautifulSoup(listing_html, "html.parser")
    # Common header/title candidates
    for selector in [
        "h1.page-title, h1.title, h1",
        "#containerId h1",
        ".breadcrumb + h1",
    ]:
        h = soup.select_one(selector)
        if h and h.get_text(strip=True):
            return h.get_text(strip=True)
    return None


def parse_institution_name(listing_html: str) -> Optional[str]:
    """Attempt to extract the institution name, often in breadcrumbs/sidebar."""
    soup = BeautifulSoup(listing_html, "html.parser")
    # Heuristics: breadcrumb first element, or labeled fields
    crumb = soup.select_one(".breadcrumb li a")
    if crumb and crumb.get_text(strip=True):
        return crumb.get_text(strip=True)
    # Fallbacks
    for selector in [
        "#containerId .institution-name",
        "[data-testid=repository-name]",
    ]:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            return el.get_text(strip=True)
    return None


def extract_pdf_titles(detail_html: str) -> List[str]:
    """Extract possible PDF titles from a document detail page.

    We look for anchor/link texts near PDFs; fallback to page title.
    """
    soup = BeautifulSoup(detail_html, "html.parser")
    titles: List[str] = []
    # Anchor text with .pdf links
    for a in soup.find_all("a", href=True):
        href = a["href"].lower().strip()
        if any(h in href for h in PDF_HINTS):
            text = a.get_text(" ", strip=True)
            if text and text not in titles:
                titles.append(text)
    # Fallback to page title
    if not titles:
        t = soup.find("title")
        if t and t.get_text(strip=True):
            titles.append(t.get_text(strip=True))
    return titles


def parse_total_pages(listing_html: str, docs_per_page: int = 30) -> Optional[int]:
    """Attempt to get the total number of pages with explicit markers first.

    Priority:
    1) Parse JS variables present in listing pages: `var maxPages = 'N';`
       If present, return N.
    2) Fallback to `itemsNum` and `pageSize` JS variables: ceil(itemsNum/pageSize).
    3) Fallback to English `pages: N` text.
    4) Fallback to Greek `τεκμήρια` count -> ceil(total/30).
    """
    import re
    # Work on the raw HTML to capture JS variables
    html = listing_html or ""
    # 1) Explicit JS maxPages marker
    m_js = re.search(r"var\s+maxPages\s*=\s*['\"](\d+)['\"]\s*;", html)
    if m_js:
        try:
            return int(m_js.group(1))
        except ValueError:
            pass
    # 2) JS itemsNum and pageSize
    m_items = re.search(r"var\s+itemsNum\s*=\s*['\"](\d+)['\"]\s*;", html)
    m_page_size = re.search(r"var\s+pageSize\s*=\s*['\"](\d+)['\"]\s*;", html)
    if m_items and m_page_size:
        try:
            items = int(m_items.group(1))
            size = max(1, int(m_page_size.group(1)))
            return max(1, math.ceil(items / size))
        except ValueError:
            pass
    # 3) English 'pages: N' within visible text
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    m = re.search(r"pages\s*:\s*(\d+)", text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    # 4) Greek 'τεκμήρια' total count
    m2 = re.search(r"([\d\.,]+)\s+τεκμήρια", text)
    if m2:
        num = m2.group(1).replace(".", "").replace(",", "")
        try:
            total = int(num)
            return max(1, math.ceil(total / max(1, docs_per_page)))
        except ValueError:
            pass
    return None


