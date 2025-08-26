"""Fast scraper limited to the first institution's first collection.

Collects all EDM document URLs across all pages using requests and BeautifulSoup.
"""

from typing import List, Tuple
import time

from .http_client import create_session, fetch_html, fetch_html_with_meta
from .parsers import (
    INSTITUTIONS_URL,
    find_first_collection_url,
    find_first_institution_url,
    find_first_collection_from_institution,
    extract_document_links,
    find_next_page_url,
    parse_collection_name,
    parse_institution_name,
    extract_pdf_titles,
    build_search_page_url,
    parse_total_pages,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def run_first_collection(max_pages: int | None = None) -> List[str]:
    session = create_session()

    # Step 1: Institutions page → first institution → its first collection
    institutions_html = fetch_html(session, INSTITUTIONS_URL)
    first_inst_url = find_first_institution_url(institutions_html, INSTITUTIONS_URL)
    if not first_inst_url:
        return []
    inst_html = fetch_html(session, first_inst_url)
    first_collection_url = find_first_collection_from_institution(inst_html, first_inst_url)
    if not first_collection_url:
        return []

    # Step 2: Paginate the collection listing via /search?page.page=N and gather document links
    all_docs: List[str] = []
    current_url = build_search_page_url(first_collection_url, 1)
    page_count = 0
    t0 = time.time()

    while current_url:
        page_html, status, elapsed = fetch_html_with_meta(session, current_url)
        total_pages = parse_total_pages(page_html) if page_count == 0 else None
        docs = extract_document_links(page_html, current_url)
        for d in docs:
            if d not in all_docs:
                all_docs.append(d)

        # Parse names (best-effort) and PDF titles for this page
        inst_name = parse_institution_name(page_html) or "(unknown institution)"
        coll_name = parse_collection_name(page_html) or "(unknown collection)"

        print(f"Page {page_count + 1} | {status} | {elapsed:.3f}s → {current_url}")
        print(f"Institution: {inst_name}")
        print(f"Collection:  {coll_name}")

        # Concurrently fetch each document page and extract likely PDF titles
        pdf_titles_for_page: List[str] = []
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = {pool.submit(fetch_html_with_meta, session, url): url for url in docs}
            for fut in as_completed(futures):
                doc_url = futures[fut]
                try:
                    html, doc_status, doc_elapsed = fut.result()
                    titles = extract_pdf_titles(html)
                    for t in titles:
                        if t not in pdf_titles_for_page:
                            pdf_titles_for_page.append(t)
                except Exception:
                    continue

        if pdf_titles_for_page:
            print("PDF titles on page:")
            for t in pdf_titles_for_page:
                print(f"  - {t}")

        page_count += 1
        if max_pages and page_count >= max_pages:
            break

        if total_pages and page_count >= total_pages:
            break

        next_url = find_next_page_url(page_html, current_url)
        if not next_url or next_url == current_url:
            break
        current_url = next_url

    total_s = time.time() - t0
    print(f"Total pages: {page_count} | Total documents: {len(all_docs)} | Runtime: {total_s:.3f}s")
    return all_docs


