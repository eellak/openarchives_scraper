#!/usr/bin/env python3
"""Merge all shard parquets into master atomically, with delta summary.

Computes the exact number of new status=200 rows the merge will introduce
relative to the current master, using the same best-of logic as the orchestrator:

- Best row per key = (collection_slug, page_number, edm_url)
- Preference: 200 over non-200, and among equals keep the last with highest retries

Usage:
  .venv/bin/python scripts/merge_shards_atomic.py \
    --master openarchives_results/external_pdfs.parquet \
    --shards-dir openarchives_results/shards \
    [--dry-run]
"""

from __future__ import annotations

import argparse
from pathlib import Path
import os
from typing import Iterable, List

import pandas as pd


KEY = ["collection_slug", "page_number", "edm_url"]


def atomic_write_parquet(path: Path, df: pd.DataFrame, compression: str = "snappy") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False, compression=compression)
    os.replace(tmp, path)


def read_parquets(paths: Iterable[Path]) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    for p in paths:
        try:
            if p.exists() and p.stat().st_size > 0:
                frames.append(pd.read_parquet(p))
        except Exception:
            # Skip unreadable shard; continue safely
            pass
    return frames


def best_of(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d["_ok"] = (d["status_code"] == 200).astype(int)
    if "retries" not in d.columns:
        d["retries"] = 0
    d["retries"] = d["retries"].fillna(0).astype(int)
    # Sort ascending on _ok so 0 comes first then 1; keep='last' picks 200 when present.
    # For equal _ok, higher retries are later and win.
    d = d.sort_values(KEY + ["_ok", "retries"]).drop_duplicates(subset=KEY, keep="last")
    return d.drop(columns=["_ok"], errors="ignore")


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge shards into master atomically with delta computation")
    ap.add_argument("--master", required=True)
    ap.add_argument("--shards-dir", required=True)
    ap.add_argument("--compression", default="snappy")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    master_path = Path(args.master)
    shards_dir = Path(args.shards_dir)

    # Gather shard files
    shard_files = sorted([p for p in shards_dir.glob("external_pdfs__*.parquet")])
    if not shard_files:
        print("[merge] no shard files found; nothing to do")
        return

    # Read master and compute current best
    frames: List[pd.DataFrame] = []
    master_best = pd.DataFrame()
    if master_path.exists() and master_path.stat().st_size > 0:
        try:
            master_df = pd.read_parquet(master_path)
            master_best = best_of(master_df)
            frames.append(master_df)
        except Exception:
            # If unreadable, treat as empty
            master_best = pd.DataFrame(columns=KEY + ["status_code", "retries"])  # minimal
    # Read shards
    frames.extend(read_parquets(shard_files))
    if not frames:
        print("[merge] no readable frames; aborting")
        return

    # Compute delta in 200s before writing
    if master_best.empty:
        current_200 = 0
    else:
        current_200 = int((master_best["status_code"] == 200).sum())

    combined = pd.concat(frames, ignore_index=True)
    combined_best = best_of(combined)
    future_200 = int((combined_best["status_code"] == 200).sum())
    delta_200 = int(future_200 - current_200)

    print(f"[merge] current_200={current_200} future_200={future_200} delta_200={delta_200}")

    if args.dry_run:
        print("[merge] dry-run: not writing master")
        return

    # Atomic write of the merged best
    atomic_write_parquet(master_path, combined_best, compression=str(args.compression))
    print(f"[merge] wrote master -> {master_path}")


if __name__ == "__main__":
    main()

