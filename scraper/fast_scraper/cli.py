#!/usr/bin/env python3
"""CLI for fast OpenArchives first-collection scraper."""

import argparse
import json
from typing import Any, Dict

from .first_collection_scraper import run_first_collection


def main():
    parser = argparse.ArgumentParser(description="Fast OpenArchives scraper (first collection only)")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit number of listing pages to scan")
    parser.add_argument("--out", type=str, default=None, help="Optional JSON file to write URLs")
    args = parser.parse_args()

    urls = run_first_collection(max_pages=args.max_pages)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(urls, fh, indent=2, ensure_ascii=False)
        print(f"Wrote {len(urls)} URLs to {args.out}")
    else:
        for u in urls:
            print(u)
        print(f"Total URLs: {len(urls)}")


if __name__ == "__main__":
    main()



