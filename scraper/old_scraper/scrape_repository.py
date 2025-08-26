"""Repository pagination module.

Iterates through all result pages of a given repository and returns the list
of document detail page URLs. It relies on CSS selectors specified in
mapping.txt but falls back to sensible defaults when those selectors fail.
"""
import logging
import re
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils import human_delay, safe_get

DOC_LINK_PATTERN = re.compile(r"/aggregator-openarchives/edm/")

# Selector hinted in mapping.txt for the pagination container – currently used
# only as a heuristic when deciding if we reached the end.
PAGINATION_CONTAINER = (
    "#edmSearchForm > div > div.col-xs-12.col-sm-12.col-md-9 > "
    "div:nth-child(2) > div.col-xs-12.col-sm-6.col-md-6.text-right"
)


def scrape_repository(
    driver,
    wait,
    repo_url: str,
    max_docs: Optional[int] = None,
) -> List[str]:
    """Collect document URLs for a repository, respecting an optional max_docs."""
    logger = logging.getLogger("openarchives")

    docs: List[str] = []

    # Step 1: Load first page normally
    logger.info("[%s] Loading initial page", repo_url)
    page_source = safe_get(driver, repo_url)

    current_page_num = 1

    def extract_doc_links(html: str) -> List[str]:
        soup_local = BeautifulSoup(html, "html.parser")
        links_local = []
        form = soup_local.select_one("#edmSearchForm")
        if form:
            anchors = form.find_all("a", href=True)
        else:
            anchors = soup_local.find_all("a", href=True)
        for a in anchors:
            if DOC_LINK_PATTERN.search(a["href"]):
                links_local.append(urljoin(repo_url, a["href"]))
        return list(dict.fromkeys(links_local))

    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        links = extract_doc_links(driver.page_source)

        if not links:
            logger.info("[%s] No document links on page %s – stopping", repo_url, current_page_num)
            break

        for link in links:
            if max_docs and len(docs) >= max_docs:
                break
            if link not in docs:
                docs.append(link)

        if max_docs and len(docs) >= max_docs:
            logger.info("[%s] Reached max_docs=%s", repo_url, max_docs)
            break

        # locate next button and active page number
        try:
            next_li = driver.find_element(By.CSS_SELECTOR, "#topPagination li.next")
            if "disabled" in next_li.get_attribute("class"):
                logger.info("[%s] Next button disabled – last page reached", repo_url)
                break

            next_anchor = next_li.find_element(By.TAG_NAME, "a")
            next_anchor.click()

            # wait until active page number increments
            current_page_num += 1
            WebDriverWait(driver, 10).until(
                lambda d: str(current_page_num)
                == d.find_element(By.CSS_SELECTOR, "#topPagination li.page.active a").text
            )
            human_delay()
        except Exception as exc:
            logger.warning("[%s] Could not click next: %s", repo_url, exc)
            break

    logger.info("[%s] Collected %d document URLs", repo_url, len(docs))
    return docs 