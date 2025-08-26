"""Institution discovery module.

Starts from the OpenArchives institutions page and collects all repository URLs
that will later be scraped individually.
"""
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import logging

from utils import human_delay, safe_get

INSTITUTIONS_URL = (
    "https://www.openarchives.gr/aggregator-openarchives/portal/institutions"
)


def discover_repositories(driver, wait, start_url: str = INSTITUTIONS_URL) -> List[str]:
    """Return a list of repository URLs discovered from the institutions page."""
    logger = logging.getLogger("openarchives")
    logger.info("Discovering repositories from %s", start_url)

    page_source = safe_get(driver, start_url)
    soup = BeautifulSoup(page_source, "html.parser")
    repo_urls = set()

    # Institution cards link to collection pages which host repositories
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/aggregator-openarchives/portal/collections/" in href:
            repo_urls.add(urljoin(start_url, href))

    logger.info("Discovered %d repositories", len(repo_urls))
    return sorted(repo_urls) 