"""Provider link PDF scanner module."""
import logging
import re
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from utils import human_delay, safe_get

PDF_REGEX = re.compile(r"\.pdf(?:\?|$)", re.IGNORECASE)
BITSTREAM_REGEX = re.compile(r"/bitstreams/.+/download", re.IGNORECASE)


def _scan_single_page(driver, provider_link: str) -> List[str]:
    logger = logging.getLogger("openarchives")
    logger.debug("Scanning provider page %s", provider_link)

    try:
        page_source = safe_get(driver, provider_link)
        soup = BeautifulSoup(page_source, "html.parser")
        pdfs: List[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if PDF_REGEX.search(href) or BITSTREAM_REGEX.search(href):
                pdfs.append(urljoin(provider_link, href))
        return list(dict.fromkeys(pdfs))
    except Exception as exc:
        logger.error("Provider scan error at %s: %s", provider_link, exc)
        return []


def scan_provider_links(driver, provider_links: List[str]) -> List[str]:
    """Return a deduplicated list of PDF links gathered from provider pages."""
    all_pdfs: List[str] = []
    for link in provider_links:
        for pdf in _scan_single_page(driver, link):
            if pdf not in all_pdfs:
                all_pdfs.append(pdf)
    return all_pdfs 