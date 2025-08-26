#!/usr/bin/env python3
"""Parallel orchestration for per-collection crawls.

Goals:
- Run N collections in parallel safely (no shared writes)
- Write per-collection shard Parquet files, then merge into master
- Limit overall CPU/network engagement via per-process concurrency

Usage (example):
  .venv/bin/python scripts/parallel_collections.py \
    --edm openarchives_results/edm_metadata_labeled.parquet \
    --collections openarchives_results/collections_links_enriched_ge1k_sorted.parquet \
    --out openarchives_results/external_pdfs.parquet \
    --parallel 5 --per-collection-limit 200 --retries 1 \
    --per-concurrency 2 --wait 0.3 --exclude dias uowm
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import os
import pandas as pd


def timestamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def select_candidate_slugs(collections_parquet: Path, edm_parquet: Path, k: int, exclude: List[str]) -> List[str]:
    df_cols = pd.read_parquet(collections_parquet)
    for col in ["item_count", "pct_ELL", "pct_NONE"]:
        if col not in df_cols.columns:
            df_cols[col] = 0.0
    mask = (df_cols["item_count"].fillna(0) >= 1000) & (
        (df_cols["pct_ELL"].fillna(0) > 0) | (df_cols["pct_NONE"].fillna(0) > 0)
    )
    cand = (
        df_cols.loc[mask, ["_slug", "item_count"]]
        .dropna()
        .sort_values("item_count", ascending=False)
        .reset_index(drop=True)
    )
    slugs = [s for s in cand["_slug"].astype(str).tolist() if s not in set(exclude)]

    # Ensure the slug actually has rows with external_link in EDM parquet
    df_edm = pd.read_parquet(edm_parquet, columns=["collection_slug", "external_link"])  # type: ignore
    df_edm = df_edm[(df_edm["collection_slug"].notna()) & (df_edm["external_link"].notna())]
    ed_slugs = set(df_edm["collection_slug"].astype(str).unique().tolist())
    final = [s for s in slugs if s in ed_slugs]
    return final[:k]


def launch_crawl(
    venv_python: Path,
    slug: str,
    edm: Path,
    collections: Path,
    out_dir: Path,
    limit: int,
    retries: int,
    per_concurrency: int,
    wait: float,
    force_http1: bool = False,
    ua_mode: str = "fixed",
    baseline: Path | None = None,
) -> Tuple[subprocess.Popen, Path]:
    out_path = out_dir / f"external_pdfs__{slug}__{timestamp()}.parquet"
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"parallel_{slug}_{timestamp()}.log"
    cmd = [
        str(venv_python), "-m", "scraper.fast_scraper.crawl_external_pdfs",
        "--edm", str(edm.resolve()),
        "--collections", str(collections.resolve()),
        "--out", str(out_path.resolve()),
        "--only-collections", slug,
        "--limit", str(int(limit)),
        "--retries", str(int(retries)),
        "--concurrency", str(int(per_concurrency)),
        "--wait", str(float(wait)),
        "--timeout", "3.0",
        "--log-level", "INFO",
        "--checkpoint-every", "5000",
        "--checkpoint-seconds", "60",
    ]
    if baseline:
        cmd.extend(["--resume-baseline", str(baseline.resolve())])
    if force_http1:
        cmd.append("--force-http1")
    if ua_mode == "random":
        cmd.append("--random-user-agent")
    elif ua_mode == "per_domain":
        cmd.append("--per-domain-user-agent")
    # Direct stdout/stderr to log
    fh = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT)
    return proc, out_path


def merge_parquets(master_out: Path, shard_paths: Iterable[Path]) -> None:
    # Read existing master if present
    frames: List[pd.DataFrame] = []
    if master_out.exists():
        try:
            frames.append(pd.read_parquet(master_out))
        except Exception:
            pass
    for p in shard_paths:
        if p.exists() and p.stat().st_size > 0:
            try:
                frames.append(pd.read_parquet(p))
            except Exception:
                pass
    if not frames:
        return
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return
    key = ["collection_slug", "page_number", "edm_url"]
    df["_ok"] = (df["status_code"] == 200).astype(int)
    if "retries" not in df.columns:
        df["retries"] = 0
    df["retries"] = df["retries"].fillna(0).astype(int)
    df = df.sort_values(key + ["_ok", "retries"]).drop_duplicates(subset=key, keep="last")
    df = df.drop(columns=["_ok"], errors="ignore")
    master_out.parent.mkdir(parents=True, exist_ok=True)
    tmp = master_out.with_suffix(master_out.suffix + ".tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    os.replace(tmp, master_out)


def analyze_errors(master_out: Path) -> pd.DataFrame:
    df = pd.read_parquet(master_out, columns=["collection_slug", "status_code"])  # type: ignore
    if df.empty:
        return df
    grp = df.groupby("collection_slug")["status_code"]
    tot = grp.size().rename("total")
    ok = (grp.apply(lambda s: (s == 200).sum())).rename("ok")
    fail = tot - ok
    err_pct = (fail / tot).mul(100.0).rename("error_pct")
    top_err = grp.apply(lambda s: s[s != 200].value_counts().head(1).to_dict()).rename("top_error")
    out = pd.concat([tot, ok, fail, err_pct, top_err], axis=1).reset_index()
    return out.sort_values("error_pct", ascending=False)


def backup_master(master_out: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = timestamp()
    dst = backup_dir / f"external_pdfs_{ts}.parquet"
    if master_out.exists():
        shutil.copy2(master_out, dst)
    return dst


def main() -> None:
    ap = argparse.ArgumentParser(description="Parallel per-collection crawler orchestrator")
    ap.add_argument("--edm", required=True)
    ap.add_argument("--collections", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--parallel", type=int, default=5)
    ap.add_argument("--per-collection-limit", type=int, default=200)
    ap.add_argument("--per-concurrency", type=int, default=2)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--wait", type=float, default=0.3)
    ap.add_argument("--exclude", nargs="*", default=["dias", "uowm"])  # known skips
    ap.add_argument("--force-http1", action="store_true")
    ap.add_argument("--baseline", default="openarchives_results/external_pdfs.parquet", help="Master parquet used to skip already-OK URLs")
    ap.add_argument("--ua-mode", default="random", choices=["fixed","random","per_domain"], help="User-Agent mode for child crawlers")
    ap.add_argument("--shards-dir", default="openarchives_results/shards")
    ap.add_argument("--backup-dir", default="openarchives_results/backups")
    args = ap.parse_args()

    edm = Path(args.edm)
    cols = Path(args.collections)
    master_out = Path(args.out)
    shards_dir = Path(args.shards_dir)
    shards_dir.mkdir(parents=True, exist_ok=True)
    backup_dir = Path(args.backup_dir)

    print("Selecting candidate slugs...", flush=True)
    slugs = select_candidate_slugs(cols, edm, k=int(args.parallel), exclude=list(args.exclude))
    print(f"Chosen slugs ({len(slugs)}): {' '.join(slugs)}", flush=True)

    print("Backing up master parquet...", flush=True)
    backup_path = backup_master(master_out, backup_dir)
    print(f"Backup: {backup_path}", flush=True)

    print("Launching parallel crawls...", flush=True)
    venv_python = Path(os.environ.get("VENV_PY", ".venv/bin/python"))
    procs: List[Tuple[subprocess.Popen, Path, str]] = []
    for slug in slugs:
        proc, shard_path = launch_crawl(
            venv_python,
            slug,
            edm,
            cols,
            shards_dir,
            limit=int(args.per_collection_limit),
            retries=int(args.retries),
            per_concurrency=int(args.per_concurrency),
            wait=float(args.wait),
            force_http1=bool(args.force_http1),
            ua_mode=str(args.ua_mode),
            baseline=Path(args.baseline) if args.baseline else None,
        )
        procs.append((proc, shard_path, slug))
    # Wait for all
    for proc, shard, slug in procs:
        rc = proc.wait()
        print(f"[{slug}] exited rc={rc} shard={shard}", flush=True)

    print("Merging shards into master...", flush=True)
    merge_parquets(master_out, (p for _, p, _ in procs))
    print(f"Merged -> {master_out}", flush=True)

    print("Analyzing error rates...", flush=True)
    try:
        summary = analyze_errors(master_out)
        print(summary.head(20).to_string(index=False))
        # Simple splits
        low_err = summary[summary["error_pct"] < 1.0]
        hi_err = summary[summary["error_pct"] >= 1.0]
        print(f"Collections <1% error: {low_err['collection_slug'].tolist()}")
        print(f"Collections >=1% error: {hi_err['collection_slug'].tolist()}")
    except Exception as e:
        print(f"Analysis failed: {e}")


if __name__ == "__main__":
    main()
