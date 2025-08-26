#!/usr/bin/env python3
"""Pipeline orchestrator for the OpenArchives modular scraper."""
import argparse
import json
import os
from datetime import datetime
from typing import Set, Dict

from utils import get_driver, setup_logger
from crawl_institutions import discover_repositories
from scrape_repository import scrape_repository
from extract_metadata import extract_metadata
from scan_provider_links import scan_provider_links
from storage_writer import StorageWriter

CHECKPOINT_FILE = "openarchives_checkpoint.json"


def _load_checkpoint() -> Dict:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return {}


def _save_checkpoint(processed_docs: Set[str], processed_repos: Set[str]):
    state = {
        "processed_docs": list(processed_docs),
        "processed_repos": list(processed_repos),
        "timestamp": datetime.utcnow().isoformat(),
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(
        description="OpenArchives.gr modular web scraper (Selenium based)"
    )
    parser.add_argument("--max-docs", type=int, help="Maximum documents per repository")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headless")
    parser.add_argument(
        "--start-url",
        default="https://www.openarchives.gr/aggregator-openarchives/portal/institutions",
        help="Custom start URL for the institutions listing page",
    )
    args = parser.parse_args()

    logger = setup_logger()
    driver, wait = get_driver(headless=args.headless)
    writer = StorageWriter()

    checkpoint = _load_checkpoint()
    processed_docs: Set[str] = set(checkpoint.get("processed_docs", []))
    processed_repos: Set[str] = set(checkpoint.get("processed_repos", []))

    try:
        repositories = discover_repositories(driver, wait, args.start_url)
        for repo_url in repositories:
            if repo_url in processed_repos:
                logger.info("Skipping previously processed repository: %s", repo_url)
                continue

            doc_urls = scrape_repository(driver, wait, repo_url, args.max_docs)
            for doc_url in doc_urls:
                if doc_url in processed_docs:
                    continue

                meta = extract_metadata(driver, doc_url)
                pdf_links = scan_provider_links(driver, meta["provider_links"])

                record = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "repository_slug": repo_url.rsplit("/", 1)[-1],
                    **meta,
                    "pdf_links": pdf_links,
                }
                writer.add(record)
                processed_docs.add(doc_url)

                if len(processed_docs) % 50 == 0:
                    writer.flush()
                    _save_checkpoint(processed_docs, processed_repos)

            processed_repos.add(repo_url)
            writer.flush()
            _save_checkpoint(processed_docs, processed_repos)

    finally:
        logger.info("Cleaning up – flushing remaining records and closing browser…")
        writer.flush()
        _save_checkpoint(processed_docs, processed_repos)
        driver.quit()


if __name__ == "__main__":
    main() 