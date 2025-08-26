"""Document metadata extraction module."""
from typing import Dict, List
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import logging

from utils import human_delay, safe_get

META_SELECTOR = (
    "#containerId > div.container-fluid.content.no-padding > "
    "div:nth-child(2) > div.col-xs-12.col-sm-12.col-md-8"
)

PROVIDER_SELECTOR = (
    "#containerId > div.container-fluid.content.no-padding > "
    "div:nth-child(2) > div.col-xs-12.col-sm-12.col-md-4 > "
    "div > div:nth-child(2) > div.col-md-11"
)


def extract_metadata(driver, doc_url: str) -> Dict:
    """Return a dict with raw HTML, plain text metadata and provider links."""
    logger = logging.getLogger("openarchives")
    logger.debug("Extracting metadata from %s", doc_url)

    page_source = safe_get(driver, doc_url)
    soup = BeautifulSoup(page_source, "html.parser")

    meta_container = soup.select_one(META_SELECTOR)
    meta_html = str(meta_container) if meta_container else ""
    meta_text = meta_container.get_text("\n", strip=True) if meta_container else ""

    provider_container = soup.select_one(PROVIDER_SELECTOR)
    provider_links: List[str] = []
    if provider_container:
        for a in provider_container.find_all("a", href=True):
            provider_links.append(a["href"].strip())
    provider_links = list(dict.fromkeys(provider_links))

    doc_slug = urlparse(doc_url).path.split("/")[-1]

    return {
        "document_slug": doc_slug,
        "document_url": doc_url,
        "metadata_html": meta_html,
        "metadata_text": meta_text,
        "provider_links": provider_links,
    } 