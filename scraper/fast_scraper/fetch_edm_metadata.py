#!/usr/bin/env python3
"""Fetch EDM pages and extract external links and metadata at scale.

Strict selectors only (evidence-backed):
- External link: <a class="right-column-title-link" href^="http">
- Metadata container: <div class="form-horizontal fieldset container-fluid">

Output Parquet columns:
- collection_slug, collection_url, page_number, edm_url
- status_code, elapsed_s, external_link, metadata_json, metadata_num_fields

Performance:
- Async httpx with HTTP/2, concurrency via semaphore, optional wait
- Minimal parsing, no speculative fallbacks
- Progress bar with speed, failures breakdown and ETA
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import csv

import httpx
import pandas as pd

from .edm_parser import fetch_and_parse_edm


DEFAULT_CONCURRENCY = 15
DEFAULT_TIMEOUT_S = 1.0
DEFAULT_WAIT_S = 0.0


@dataclass
class LinkRow:
    collection_slug: str
    collection_url: str
    page_number: int
    edm_url: str


def _httpx_timeout(seconds: float) -> httpx.Timeout:
    return httpx.Timeout(connect=seconds, read=seconds, write=seconds, pool=seconds)


async def run(
    in_links: Path,
    out_parquet: Path,
    limit: int,
    concurrency: int,
    timeout_s: float,
    wait_s: float,
    progress: bool,
    compression: Optional[str],
    log_dir: Optional[Path],
    metrics_chunk: int,
    metrics_out: Optional[Path],
    project_total: Optional[int],
    retries: int,
    resume: bool,
    retry_only: bool,
) -> None:
    # Configure logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    root_logger.addHandler(console)
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        fileh = logging.FileHandler(str((log_dir / f"openarchives_edm_meta_{ts}.log").resolve()), encoding="utf-8")
        fileh.setLevel(logging.INFO)
        fileh.setFormatter(formatter)
        root_logger.addHandler(fileh)

    # Read input links (minimal columns)
    req_cols = ["collection_slug", "collection_url", "page_number", "edm_url"]
    df_links = pd.read_parquet(in_links, columns=req_cols)
    if limit and limit > 0:
        df_links = df_links.head(limit)

    # Unique edm URLs for fetching
    unique_urls_all = df_links.drop_duplicates(subset=["edm_url"])["edm_url"].tolist()

    # Load existing best statuses for resume/retries
    existing_status: Dict[str, Dict[str, int]] = {}
    if resume and out_parquet.exists():
        try:
            old = pd.read_parquet(out_parquet, columns=["edm_url", "status_code", "retries"])  # retries may not exist
            if not old.empty:
                # Vectorized best-per-url: 200 preferred over non-200, among non-200 keep highest retries
                old = old.copy()
                old["_order"] = (old["status_code"] == 200).astype(int)
                if "retries" in old.columns:
                    old["retries"] = old["retries"].fillna(0).astype(int)
                else:
                    old["retries"] = 0
                old.sort_values(["edm_url", "_order", "retries"], inplace=True)
                best = old.drop_duplicates(subset=["edm_url"], keep="last")
                existing_status = best.set_index("edm_url")[
                    ["status_code", "retries"]
                ].to_dict(orient="index")
        except Exception:
            pass

    # Determine worklist
    if resume:
        if retries > 0:
            base_list = [u for u in unique_urls_all if (u in existing_status)] if retry_only else unique_urls_all
            work_urls = [u for u in base_list if int(existing_status.get(u, {}).get("status_code", 0)) != 200]
        else:
            # Resume without retries: continue only with URLs not present in existing output
            work_urls = [u for u in unique_urls_all if u not in existing_status]
    else:
        work_urls = unique_urls_all

    total_unique = len(work_urls)
    logging.info(
        "Planning fetch: %s unique EDM URLs from %s rows (limit=%s; resume=%s, retries=%s, retry_only=%s)",
        total_unique, len(df_links), limit, bool(resume), int(retries), bool(retry_only)
    )

    # Progress counters
    start_ts = time.perf_counter()
    attempts = 0
    completed = 0
    counts: Dict[str, int] = {"200": 0, "404": 0, "500": 0, "timeouts": 0, "other": 0}

    # Metrics sampling (failure rate derivative per chunk)
    metrics_out_path: Optional[Path] = None
    metrics_writer: Optional[csv.writer] = None
    metrics_fh = None
    last_sample_fail_pct: float = 0.0
    last_sample_completed: int = 0
    if metrics_out:
        metrics_out.parent.mkdir(parents=True, exist_ok=True)
        metrics_out_path = metrics_out
    elif log_dir:
        ts = time.strftime("%Y%m%d_%H%M%S")
        metrics_out_path = (log_dir / f"edm_meta_metrics_{ts}.csv")
    if metrics_out_path:
        metrics_fh = open(metrics_out_path, "w", newline="", encoding="utf-8")
        metrics_writer = csv.writer(metrics_fh)
        metrics_writer.writerow([
            "t_epoch",
            "completed",
            "failed",
            "fail_pct",
            "delta_fail_pct_per_chunk",
            "derivative_fail_pct_per_request",
            "speed_ps",
            "project_total",
            "project_fail_pct",
            "project_fail_count",
        ])

    last_derivative: float = 0.0
    def _render_progress() -> None:
        if not progress:
            return
        elapsed = max(1e-6, time.perf_counter() - start_ts)
        speed = completed / elapsed
        failed = counts["404"] + counts["500"] + counts["timeouts"] + counts["other"]
        fail_pct = (failed / max(1, attempts)) * 100.0
        # Bar
        width = 40
        frac = 0.0 if total_unique <= 0 else min(1.0, completed / max(1, total_unique))
        filled = int(width * frac)
        bar = f"[{'#'*filled}{'.'*(width-filled)}]"
        # ETA
        remaining = max(0, total_unique - completed)
        eta_s = (remaining / speed) if speed > 0 else 0.0
        eta_m = int(eta_s // 60)
        eta_x = int(eta_s % 60)
        line = (
            f"{bar} {completed}/{total_unique} edm | {speed:.1f} p/s | "
            f"fail%: {fail_pct:.1f}% dFail%/100: {last_derivative*100:.2f} | (to={counts['timeouts']}, 404={counts['404']}, 500={counts['500']}, other={counts['other']}) | "
            f"ETA: {eta_m:02d}:{eta_x:02d}"
        )
        try:
            sys.stdout.write("\r" + line)
            sys.stdout.flush()
        except Exception:
            pass

    def _maybe_sample_metrics() -> None:
        nonlocal last_sample_fail_pct, last_sample_completed, last_derivative
        if not metrics_writer:
            return
        if completed == 0 or completed % max(1, metrics_chunk) != 0:
            return
        elapsed = max(1e-6, time.perf_counter() - start_ts)
        speed = completed / elapsed
        failed = counts["404"] + counts["500"] + counts["timeouts"] + counts["other"]
        fail_pct = (failed / max(1, completed)) * 100.0
        delta = fail_pct - last_sample_fail_pct
        deriv_per_req = delta / max(1, (completed - last_sample_completed))
        last_derivative = float(deriv_per_req)
        proj_total = int(project_total) if project_total else None
        proj_fail_pct = None
        proj_fail_count = None
        if proj_total and completed > 0:
            # Linear extrapolation based on samples so far (use current derivative)
            remaining = max(0, proj_total - completed)
            proj_fail_pct = min(100.0, max(0.0, fail_pct + deriv_per_req * remaining))
            proj_fail_count = int(round((proj_fail_pct / 100.0) * proj_total))
        metrics_writer.writerow([
            int(time.time()),
            int(completed),
            int(failed),
            round(fail_pct, 6),
            round(delta, 6),
            round(deriv_per_req, 8),
            round(speed, 6),
            proj_total if proj_total else "",
            round(proj_fail_pct, 6) if proj_fail_pct is not None else "",
            int(proj_fail_count) if proj_fail_count is not None else "",
        ])
        if metrics_fh:
            try:
                metrics_fh.flush()
            except Exception:
                pass
        last_sample_fail_pct = fail_pct
        last_sample_completed = completed

    # Async fetch
    results: Dict[str, Dict[str, object]] = {}
    async with httpx.AsyncClient(http2=True, timeout=_httpx_timeout(timeout_s), limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)) as client:
        semaphore = asyncio.Semaphore(concurrency)

        async def _fetch_once(url: str, retries_value: int) -> int:
            nonlocal attempts, completed
            attempts += 1
            try:
                r = await fetch_and_parse_edm(client, url, semaphore=semaphore, wait_s=wait_s, with_provenance=False)
                sc = int(r.get("status_code", 0))
                if sc == 200:
                    counts["200"] += 1
                elif sc == 404:
                    counts["404"] += 1
                elif sc == 500:
                    counts["500"] += 1
                elif sc < 0:
                    counts["timeouts"] += 1
                else:
                    counts["other"] += 1
                results[url] = {
                    "status_code": sc,
                    "elapsed_s": float(r.get("elapsed_s", 0.0)),
                    "external_link": r.get("external_link"),
                    "metadata_json": json.dumps(r.get("metadata") or {}, ensure_ascii=False),
                    "metadata_num_fields": len(r.get("metadata") or {}),
                    "retries": int(retries_value),
                }
            except httpx.TimeoutException:
                counts["timeouts"] += 1
                results[url] = {
                    "status_code": -1,
                    "elapsed_s": 0.0,
                    "external_link": None,
                    "metadata_json": json.dumps({}, ensure_ascii=False),
                    "metadata_num_fields": 0,
                    "retries": int(retries_value),
                }
            except Exception:
                counts["other"] += 1
                results[url] = {
                    "status_code": -2,
                    "elapsed_s": 0.0,
                    "external_link": None,
                    "metadata_json": json.dumps({}, ensure_ascii=False),
                    "metadata_num_fields": 0,
                    "retries": int(retries_value),
                }
            finally:
                completed += 1
                _render_progress()
                _maybe_sample_metrics()
            return int(results[url]["status_code"])  # type: ignore[index]

        async def _task(url: str) -> None:
            prev_r = int(existing_status.get(url, {}).get("retries", 0)) if (resume and 'existing_status' in locals()) else 0
            sc = await _fetch_once(url, prev_r)
            if sc == 200 or retries <= 0:
                return
            for add in range(1, retries + 1):
                if int(results.get(url, {}).get("status_code", 0)) == 200:
                    break
                await _fetch_once(url, prev_r + add)

        tasks = [asyncio.create_task(_task(u)) for u in work_urls]
        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except asyncio.CancelledError:
                pass

    # Newline after progress bar
    try:
        sys.stdout.write("\n")
        sys.stdout.flush()
    except Exception:
        pass

    # If nothing was attempted (e.g., resume+retry_only and nothing pending), skip write to avoid clobbering
    if len(results) == 0 and resume:
        logging.info("No URLs processed (resume=%s, retry_only=%s); skipping write.", bool(resume), bool(retry_only))
        return

    # Merge results back to original rows (preserve duplicates for per-collection context)
    attempted_set = set(results.keys())
    records = []
    if not (resume and retry_only):
        iterator = (row for _, row in df_links.iterrows())
    else:
        iterator = (r for _, r in df_links.iterrows() if r["edm_url"] in attempted_set)
    for row in iterator:
        base = {
            "collection_slug": row["collection_slug"],
            "collection_url": row["collection_url"],
            "page_number": int(row["page_number"]) if pd.notna(row["page_number"]) else 0,
            "edm_url": row["edm_url"],
        }
        rest = results.get(row["edm_url"], {
            "status_code": -3,
            "elapsed_s": 0.0,
            "external_link": None,
            "metadata_json": json.dumps({}, ensure_ascii=False),
            "metadata_num_fields": 0,
            "retries": 0,
        })
        rec = {**base, **rest}
        records.append(rec)
    df_res = pd.DataFrame.from_records(records)

    # Write/merge parquet (prefer best status: 200 > others; then highest retries)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    if out_parquet.exists():
        try:
            old = pd.read_parquet(out_parquet)
            if not old.empty:
                merged = pd.concat([old, df_res], ignore_index=True)
                key = ["collection_slug", "page_number", "edm_url"]
                merged["_status_order"] = (merged["status_code"] == 200).astype(int)
                merged["retries"] = merged.get("retries", 0).fillna(0).astype(int)
                merged = merged.sort_values(key + ["_status_order", "retries"]).drop_duplicates(subset=key, keep="last")
                merged = merged.drop(columns=["_status_order"], errors="ignore")
                merged.to_parquet(out_parquet, index=False, compression=compression or "snappy")
                logging.info("Merged %s rows (old=%s, new=%s) → %s", len(merged), len(old), len(df_res), out_parquet)
            else:
                df_res.to_parquet(out_parquet, index=False, compression=compression or "snappy")
                logging.info("Wrote %s EDM metadata rows → %s", len(df_res), out_parquet)
        except Exception:
            df_res.to_parquet(out_parquet, index=False, compression=compression or "snappy")
            logging.info("Wrote %s EDM metadata rows → %s", len(df_res), out_parquet)
    else:
        if len(df_res) > 0:
            df_res.to_parquet(out_parquet, index=False, compression=compression or "snappy")
            logging.info("Wrote %s EDM metadata rows → %s", len(df_res), out_parquet)
        else:
            logging.info("No new rows to write; output not created.")
    if metrics_fh:
        try:
            metrics_fh.close()
        except Exception:
            pass


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch EDM metadata at scale with strict selectors")
    p.add_argument("--in-links", default=str(Path("openarchives_results/all_links.parquet")), help="Input links parquet (collection/page/edm)")
    p.add_argument("--out", default=str(Path("openarchives_results/edm_metadata.parquet")), help="Output parquet path")
    p.add_argument("--limit", type=int, default=10000, help="Max rows from input to process")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    p.add_argument("--wait", type=float, default=DEFAULT_WAIT_S)
    p.add_argument("--progress", action="store_true")
    p.add_argument("--compression", default="snappy", help="Parquet compression (snappy|zstd|gzip|none)")
    p.add_argument("--log-dir", default="logs", help="Directory for detailed logs")
    p.add_argument("--metrics-chunk", type=int, default=100, help="Sample size (in completed requests) for failure-rate derivative")
    p.add_argument("--metrics-out", default=None, help="Optional CSV path to store metrics samples (default under --log-dir)")
    p.add_argument("--project-total", type=int, default=None, help="Optional projection target for failure-rate estimates (e.g., 830313)")
    p.add_argument("--retries", type=int, default=0, help="Additional retry attempts per URL in resume mode (early-stop on 200)")
    p.add_argument("--resume", action="store_true", default=False, help="Resume from existing output parquet state")
    p.add_argument("--retry-only", action="store_true", default=False, help="In resume+retries mode, process only previously non-200 URLs")
    args = p.parse_args()

    comp = None if (args.compression or "snappy").lower() == "none" else args.compression
    asyncio.run(run(
        Path(args.in_links),
        Path(args.out),
        int(args.limit),
        int(args.concurrency),
        float(args.timeout),
        float(args.wait),
        bool(args.progress),
        comp,
        Path(args.log_dir) if args.log_dir else None,
        int(args.metrics_chunk),
        Path(args.metrics_out) if args.metrics_out else None,
        int(args.project_total) if args.project_total else None,
        int(args.retries),
        bool(args.resume),
        bool(args.retry_only),
    ))


if __name__ == "__main__":
    main()


