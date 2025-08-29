#!/usr/bin/env python3
"""Adaptive parallel orchestration for crawling collections.

Features
- Rotates through all eligible collections, running up to N in parallel.
- After each shard finishes, computes error rate and (if high) runs a
  retry pass for that collection with reduced concurrency and added wait.
- Writes per-collection shard Parquet files and merges them atomically into
  the master Parquet at the end (or optionally after each wave).

Assumptions
- Child crawler module: scraper.fast_scraper.crawl_external_pdfs
- Crawler supports: --only-collections, --resume, --retries,
  --concurrency, --wait, --force-http1, UA flags, and atomic writes.

Example
  .venv/bin/python scripts/parallel_adaptive.py \
    --edm openarchives_results/edm_metadata_labeled.parquet \
    --collections openarchives_results/collections_links_enriched_ge1k_sorted.parquet \
    --out openarchives_results/external_pdfs.parquet \
    --max-parallel 10 --per-concurrency 5 --retries 0 \
    --retry-threshold 5.0 --retry-per-concurrency 3 --retry-wait 0.5 \
    --ua-mode random --force-http1
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
from dataclasses import dataclass, field
import time
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import sys

# Ensure line-buffered stdout so tail -f shows updates promptly
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def atomic_write_parquet(path: Path, df: pd.DataFrame, compression: str = "snappy") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False, compression=compression)
    os.replace(tmp, path)


def select_slugs(collections_parquet: Path, edm_parquet: Path, exclude: List[str]) -> List[str]:
    df_cols = pd.read_parquet(collections_parquet)
    for col in ["item_count", "pct_ELL", "pct_NONE"]:
        if col not in df_cols.columns:
            df_cols[col] = 0.0
    mask = (df_cols["item_count"].fillna(0) >= 1000) & (
        (df_cols["pct_ELL"].fillna(0) > 0) | (df_cols["pct_NONE"].fillna(0) > 0)
    )
    slugs = (
        df_cols.loc[mask, "_slug"].dropna().astype(str).tolist()
    )
    slugs = [s for s in slugs if s not in set(exclude)]
    # Ensure present in EDM
    df_edm = pd.read_parquet(edm_parquet, columns=["collection_slug"])  # type: ignore
    ed_slugs = set(df_edm["collection_slug"].dropna().astype(str).unique())
    return [s for s in slugs if s in ed_slugs]


@dataclass
class ChildSpec:
    slug: str
    shard_path: Path
    log_path: Path
    metrics_path: Path
    total_target: int
    start_ts: float
    last_attempted: int = 0
    last_ok: int = 0
    last_failish: int = 0
    last_ts: float = 0.0
    neg_streak: int = 0
    lowered_once: bool = False
    dropped: bool = False
    stage: int = 1  # 1: (15,0.3) 2: (10,0.5) 3: (5,1.0)
    last_attempted_at_stage: int = 0
    last_m_completed: int = 0
    last_m_ts: float = 0.0
    ema_speed: float = 0.0
    last_eta_s: float = 0.0

    # dynamic attributes like retry_passes, last_parquet_ts, etc. are added at runtime


def launch_child(
    venv_python: Path,
    edm: Path,
    collections: Path,
    shard_path: Path,
    metrics_path: Path,
    slug: str,
    *,
    concurrency: int,
    wait: float,
    retries: int,
    child_timeout_s: float,
    force_http1: bool,
    ua_mode: str,
    limit: int = 0,
    baseline: Path | None = None,
) -> subprocess.Popen:
    log_dir = Path("logs"); log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"parallel_{slug}_{ts()}.log"
    cmd = [
        str(venv_python), "-m", "scraper.fast_scraper.crawl_external_pdfs",
        "--edm", str(edm.resolve()),
        "--collections", str(collections.resolve()),
        "--out", str(shard_path.resolve()),
        "--only-collections", slug,
        "--concurrency", str(int(concurrency)),
        "--wait", str(float(wait)),
        "--timeout", str(float(child_timeout_s)),
        "--retries", str(int(retries)),
        "--log-level", "WARNING",
        "--checkpoint-every", "1000",
        "--checkpoint-seconds", "10",
        "--metrics-chunk", "250",
        "--metrics-out", str(metrics_path.resolve()),
    ]
    if limit and limit > 0:
        cmd.extend(["--limit", str(int(limit))])
    if baseline:
        cmd.extend(["--resume-baseline", str(baseline.resolve())])
    if force_http1:
        cmd.append("--force-http1")
    if ua_mode == "random":
        cmd.append("--random-user-agent")
    elif ua_mode == "per_domain":
        cmd.append("--per-domain-user-agent")
    # Emit a one-line launch summary to orchestrator stdout
    print(f"[launch] {slug}: conc={concurrency} wait={wait}s retries={retries} baseline={'yes' if baseline else 'no'} -> {shard_path}", flush=True)
    # Open per-collection log in append-binary mode and close in parent to avoid FD leaks.
    # Ensure PYTHONPATH points to project root so 'scraper' package resolves
    env = os.environ.copy()
    try:
        proj_root = Path(__file__).resolve().parents[1]
        pypp = env.get("PYTHONPATH", "")
        sep = os.pathsep
        add = str(proj_root)
        if pypp:
            if add not in pypp.split(sep):
                env["PYTHONPATH"] = add + sep + pypp
        else:
            env["PYTHONPATH"] = add
    except Exception:
        pass
    with open(log_path, "ab", buffering=0) as fh:
        proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env)
    return proc


def compute_error_rate(shard_path: Path) -> Tuple[float, dict]:
    if not shard_path.exists() or shard_path.stat().st_size == 0:
        return 100.0, {}
    df = pd.read_parquet(shard_path, columns=["status_code"])  # type: ignore
    if df.empty:
        return 100.0, {}
    # Exclude -3 (skipped/baseline) and 404 (dead links) from error% computation
    df_eff = df[(df["status_code"] != -3) & (df["status_code"] != 404)]
    if df_eff.empty:
        # everything was skipped -> 0% error
        bc = df["status_code"].value_counts().to_dict()
        return 0.0, bc
    total = len(df_eff)
    ok = int((df_eff["status_code"] == 200).sum())
    err = total - ok
    rate = (err / total) * 100.0
    bc = df["status_code"].value_counts().to_dict()
    return rate, bc


def compute_target_for_slug(edm: Path, slug: str) -> int:
    try:
        df = pd.read_parquet(edm, columns=["collection_slug","external_link","language_code"])  # type: ignore
        df = df[(df["collection_slug"].astype(str)==slug) & df["external_link"].notna() & df["language_code"].isin(["ELL","NONE"])]
        return int(df["external_link"].astype(str).dropna().nunique())
    except Exception:
        return 0


def read_attempted_counts(shard_path: Path) -> Tuple[int,int,int,int,int]:
    """Return (attempted, ok, s404, s500, timeouts_or_neg)."""
    if not shard_path.exists() or shard_path.stat().st_size == 0:
        return 0,0,0,0,0
    try:
        df = pd.read_parquet(shard_path, columns=["status_code"])  # type: ignore
        if df.empty:
            return 0,0,0,0,0
        attempted = len(df)
        ok = int((df["status_code"]==200).sum())
        s404 = int((df["status_code"]==404).sum())
        s500 = int((df["status_code"]==500).sum())
        tout_other = int((df["status_code"]<0).sum())
        return attempted, ok, s404, s500, tout_other
    except Exception:
        return 0,0,0,0,0

def read_status_breakdown(shard_path: Path) -> dict:
    try:
        if not shard_path.exists() or shard_path.stat().st_size == 0:
            return {}
        df = pd.read_parquet(shard_path, columns=["status_code"])  # type: ignore
        if df.empty:
            return {}
        vc = df["status_code"].value_counts().to_dict()
        return {int(k): int(v) for k,v in vc.items()}
    except Exception:
        return {}

def read_unique_breakdown(shard_path: Path) -> Tuple[int,int,int,int,int,dict]:
    """Return unique-URL breakdown using the best status per URL.

    Returns: (attempted_unique, ok, s404, s5xx, neg, counts_by_code_best)
    Notes: ignores -3 (skipped) rows when selecting best; ranking prefers
    200 > 404 > 5xx > negatives (<0).
    """
    try:
        if not shard_path.exists() or shard_path.stat().st_size == 0:
            return 0,0,0,0,0,{}
        df = pd.read_parquet(shard_path, columns=["external_link","status_code"])  # type: ignore
        if df.empty:
            return 0,0,0,0,0,{}
        # remove baseline-skipped rows from consideration
        df = df[df["status_code"] != -3]
        if df.empty:
            return 0,0,0,0,0,{}
        def _rank(sc: int) -> int:
            if sc == 200: return 0
            if sc == 404: return 1
            if sc >= 500: return 2
            if sc < 0:    return 3
            return 4
        df["_r"] = df["status_code"].map(_rank)
        best = df.sort_values(["external_link","_r"]).drop_duplicates("external_link", keep="first")
        attempted = len(best)
        ok  = int((best["status_code"] == 200).sum())
        s404 = int((best["status_code"] == 404).sum())
        s5xx = int((best["status_code"] >= 500).sum())
        neg = int((best["status_code"] < 0).sum())
        bc = best["status_code"].value_counts().to_dict()
        return attempted, ok, s404, s5xx, neg, {int(k): int(v) for k,v in bc.items()}
    except Exception:
        return 0,0,0,0,0,{}


def read_metrics_counts(metrics_path: Path) -> Tuple[int, float]:
    """Return (completed, eta_s) from metrics CSV by tailing last line.

    Uses an O(1) tail read to avoid scanning the whole file.
    """
    try:
        if not metrics_path.exists():
            return 0, 0.0
        with open(metrics_path, 'rb') as fh:
            try:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                # Read last up to 8 KB
                read_size = min(8192, size)
                fh.seek(-read_size, os.SEEK_END)
                chunk = fh.read(read_size)
            except OSError:
                fh.seek(0)
                chunk = fh.read()
        # Decode and get last non-empty line that is not header
        text = chunk.decode('utf-8', errors='ignore')
        lines = [ln for ln in text.strip().splitlines() if ln.strip()]
        if not lines:
            return 0, 0.0
        # Find last CSV line with at least 3 commas (7 fields header/body)
        last = None
        for ln in reversed(lines):
            if ln.lower().startswith('t_epoch'):  # header
                continue
            if ln.count(',') >= 6:
                last = ln
                break
        if not last:
            return 0, 0.0
        parts = [p.strip() for p in last.split(',')]
        completed = int(float(parts[1])) if len(parts) > 1 else 0
        eta_s = float(parts[5]) if len(parts) > 5 and parts[5] else 0.0
        return completed, eta_s
    except Exception:
        return 0, 0.0


def read_metrics_snapshot(metrics_path: Path) -> Tuple[int, float, float, int]:
    """Return (completed, eta_s, speed_ps, remaining) from last CSV line.

    Falls back to zeros on any issue; reads last ~8KB to avoid scanning.
    """
    try:
        if not metrics_path.exists():
            return 0, 0.0, 0.0, 0
        with open(metrics_path, 'rb') as fh:
            try:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                read_size = min(8192, size)
                fh.seek(-read_size, os.SEEK_END)
                chunk = fh.read(read_size)
            except OSError:
                fh.seek(0)
                chunk = fh.read()
        text = chunk.decode('utf-8', errors='ignore')
        lines = [ln for ln in text.strip().splitlines() if ln.strip()]
        if not lines:
            return 0, 0.0, 0.0, 0
        last = None
        for ln in reversed(lines):
            if ln.lower().startswith('t_epoch'):
                continue
            if ln.count(',') >= 6:  # expect 7 cols
                last = ln
                break
        if not last:
            return 0, 0.0, 0.0, 0
        parts = [p.strip() for p in last.split(',')]
        completed = int(float(parts[1])) if len(parts) > 1 else 0
        speed_ps = float(parts[4]) if len(parts) > 4 and parts[4] else 0.0
        eta_s = float(parts[5]) if len(parts) > 5 and parts[5] else 0.0
        remaining = int(float(parts[6])) if len(parts) > 6 and parts[6] else 0
        return completed, eta_s, speed_ps, remaining
    except Exception:
        return 0, 0.0, 0.0, 0


def merge_master(master_out: Path, shards: Iterable[Path]) -> None:
    frames: List[pd.DataFrame] = []
    if master_out.exists():
        try:
            frames.append(pd.read_parquet(master_out))
        except Exception:
            pass
    for p in shards:
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
    atomic_write_parquet(master_out, df, compression="snappy")


def main() -> None:
    ap = argparse.ArgumentParser(description="Adaptive parallel collection crawler")
    ap.add_argument("--edm", required=True)
    ap.add_argument("--collections", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max-parallel", type=int, default=10)
    ap.add_argument("--per-concurrency", type=int, default=15)
    ap.add_argument("--wait", type=float, default=0.3)
    ap.add_argument("--timeout", type=float, default=3.0)
    ap.add_argument("--retries", type=int, default=0)
    ap.add_argument("--retry-threshold", type=float, default=5.0, help="If error% >= threshold, run a retry pass")
    ap.add_argument("--retry-per-concurrency", type=int, default=10)
    ap.add_argument("--retry-wait", type=float, default=0.5)
    ap.add_argument("--max-retry-passes", type=int, default=2, help="Max number of post-run retry passes per collection when error% exceeds threshold")
    ap.add_argument("--baseline", default="openarchives_results/external_pdfs.parquet", help="Master parquet used to skip already-OK URLs")
    ap.add_argument("--exclude", nargs="*", default=["dias"])  # default skip
    ap.add_argument("--ua-mode", default="random", choices=["fixed","random","per_domain"])
    ap.add_argument("--force-http1", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="Optional capped input per collection during testing")
    ap.add_argument("--merge-each-wave", action="store_true")
    ap.add_argument("--rolling", action="store_true", help="Top up pool to max-parallel by launching next slugs as soon as one finishes")
    ap.add_argument("--only-slugs", nargs="*", default=None, help="If provided, restrict run to these collection slugs (after selection & exclude)")
    ap.add_argument("--overrides-json", default=None, help="Optional JSON file with per-slug overrides: {slug: {per_concurrency, wait, timeout, retries, ua_mode, force_http1, retry_per_concurrency, retry_wait}}")
    args = ap.parse_args()

    edm = Path(args.edm)
    cols = Path(args.collections)
    master_out = Path(args.out)
    shards_dir = Path("openarchives_results/shards"); shards_dir.mkdir(parents=True, exist_ok=True)
    venv_python = Path(os.environ.get("VENV_PY", ".venv/bin/python"))

    slugs = select_slugs(cols, edm, exclude=list(args.exclude))
    if args.only_slugs:
        try:
            only_set = set([s for s in args.only_slugs if s])
            slugs = [s for s in slugs if s in only_set]
        except Exception:
            pass

    # Load per-slug overrides if provided
    overrides: dict[str, dict] = {}
    if args.overrides_json:
        try:
            import json
            with open(args.overrides_json, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
                if isinstance(data, dict):
                    overrides = {str(k): dict(v) for k, v in data.items()}
        except Exception:
            overrides = {}

    def _eff(slug: str, key: str, default):
        try:
            ov = overrides.get(str(slug), {})
            val = ov.get(key, None)
            return default if (val is None) else val
        except Exception:
            return default
    # Precompute target unique counts per slug once to avoid repeated parquet scans
    # Also compute residual targets after subtracting baseline 200s to align progress/ETA
    tgt_by_slug: dict[str, int] = {}
    residual_by_slug: dict[str, int] = {}
    try:
        df_tgt = pd.read_parquet(edm, columns=["collection_slug","external_link","language_code"])  # type: ignore
        df_tgt = df_tgt[df_tgt["language_code"].isin(["ELL","NONE"]) & df_tgt["external_link"].notna()]
        df_tgt["collection_slug"] = df_tgt["collection_slug"].astype(str)
        tgt_by_slug = (
            df_tgt.groupby("collection_slug")["external_link"].nunique().astype(int).to_dict()
        )
        base_ok_by_slug: dict[str, int] = {}
        try:
            base_path = Path(args.baseline) if args.baseline else None
            if base_path and base_path.exists():
                # Try to prefer rows that already have PDFs in baseline; fall back to all 200s
                base_cols = ["collection_slug","external_link","status_code","pdf_links_count"]
                try:
                    base = pd.read_parquet(base_path, columns=base_cols)  # type: ignore
                except Exception:
                    base = pd.read_parquet(base_path, columns=base_cols[:3])  # type: ignore
                    base["pdf_links_count"] = 0
                if not base.empty:
                    base = base[base["external_link"].notna()].copy()
                    base["collection_slug"] = base["collection_slug"].astype(str)
                    # Count only those URLs that already have PDFs if the column exists; else count 200s
                    if "pdf_links_count" in base.columns:
                        b2 = base[(base["status_code"] == 200) & (base.get("pdf_links_count", 0).fillna(0).astype(int) > 0)].copy()
                    else:
                        b2 = base[(base["status_code"] == 200)].copy()
                    base_ok_by_slug = b2.groupby("collection_slug")["external_link"].nunique().astype(int).to_dict()
        except Exception:
            base_ok_by_slug = {}
        residual_by_slug = {s: max(0, int(tgt_by_slug.get(s, 0)) - int(base_ok_by_slug.get(s, 0))) for s in tgt_by_slug.keys()}
    except Exception:
        tgt_by_slug = {}
        residual_by_slug = {}
    run_started = time.perf_counter()
    print(f"[run] started ts={dt.datetime.now().isoformat()} max_parallel={args.max_parallel} per_conc={args.per_concurrency} wait={args.wait} retries={args.retries} limit={args.limit} baseline={args.baseline}", flush=True)
    print(f"Total candidate slugs: {len(slugs)}", flush=True)

    # Rotate through slugs in waves up to max-parallel
    i = 0
    done_shards: List[Path] = []
    while i < len(slugs):
        wave = slugs[i:i + int(args.max_parallel)]
        print(f"Launching wave: {wave}", flush=True)
        procs: List[Tuple[subprocess.Popen, ChildSpec]] = []
        # Track all ChildSpec objects started in this wave, including rolling top-ups
        wave_specs: List[ChildSpec] = []
        for s in wave:
            tstr = ts()
            shard_path = shards_dir / f"external_pdfs__{s}__{tstr}.parquet"
            metrics_path = Path("logs") / f"metrics_{s}_{tstr}.csv"
            total_tgt = int(residual_by_slug.get(s, tgt_by_slug.get(s, 0)))
            # Effective per-slug settings (overrides fall back to CLI defaults)
            eff_conc = int(_eff(s, 'per_concurrency', int(args.per_concurrency)))
            eff_wait = float(_eff(s, 'wait', float(args.wait)))
            eff_timeout = float(_eff(s, 'timeout', float(args.timeout)))
            eff_retries = int(_eff(s, 'retries', int(args.retries)))
            eff_force_h1 = bool(_eff(s, 'force_http1', bool(args.force_http1)))
            eff_ua = str(_eff(s, 'ua_mode', str(args.ua_mode)))
            eff_limit = int(_eff(s, 'limit', int(args.limit))) if hasattr(args, 'limit') else int(args.limit)
            proc = launch_child(
                venv_python, edm, cols, shard_path, metrics_path, s,
                concurrency=eff_conc, wait=eff_wait, retries=eff_retries, child_timeout_s=eff_timeout,
                force_http1=eff_force_h1, ua_mode=eff_ua, limit=eff_limit, baseline=Path(args.baseline) if args.baseline else None,
            )
            now = time.perf_counter()
            spec0 = ChildSpec(
                slug=s,
                shard_path=shard_path,
                log_path=Path("logs")/f"parallel_{s}_{tstr}.log",
                metrics_path=metrics_path,
                total_target=total_tgt,
                start_ts=now,
                last_attempted=0,
                last_ts=now,
                stage=1,
                last_attempted_at_stage=0,
                last_m_ts=now,
            )
            procs.append((proc, spec0))
            wave_specs.append(spec0)

        # Periodic minimal status while children running
        alive = {id(proc): (proc, spec) for proc, spec in procs}
        # Index of next slug to launch if --rolling is enabled (keeps pool full)
        next_idx = i + len(wave)
        # Setup for graceful shutdown
        import signal
        shutdown = {"flag": False}
        def _sig_handler(signum, frame):
            shutdown["flag"] = True
        try:
            signal.signal(signal.SIGINT, _sig_handler)
            signal.signal(signal.SIGTERM, _sig_handler)
        except Exception:
            pass
        # Status cadence (seconds); default 10 but configurable via env
        try:
            status_interval = float(os.environ.get("STATUS_INTERVAL_S", "10"))
        except Exception:
            status_interval = 10.0
        while alive:
            # Handle shutdown request
            if shutdown.get("flag"):
                print("[run] shutdown requested; terminating children...", flush=True)
                for _pid, (p, _sp) in list(alive.items()):
                    try:
                        p.terminate()
                    except Exception:
                        pass
                for _pid, (p, _sp) in list(alive.items()):
                    try:
                        p.wait(timeout=5)
                    except Exception:
                        pass
                break

            # Print per-collection minimal lines and global ETA
            g_total_rem = 0
            g_speed = 0.0
            for pid, (proc, spec) in list(alive.items()):
                if proc.poll() is not None:
                    # finished
                    rc = proc.returncode
                    print(f"[{spec.slug}] finished rc={rc} shard={spec.shard_path}", flush=True)
                    rate, bc = compute_error_rate(spec.shard_path)
                    print(f"[{spec.slug}] error%={rate:.2f} breakdown={bc}", flush=True)
                    # Record shard for final merge (avoid duplicates)
                    if spec.shard_path not in done_shards:
                        done_shards.append(spec.shard_path)
                    # Determine if unique work is already complete; if so, do not schedule post-run retries
                    try:
                        attempted_u_fin, _ok_u_fin, _s404_u_fin, _s5xx_u_fin, _neg_u_fin, _bc_best_fin = read_unique_breakdown(spec.shard_path)
                    except Exception:
                        attempted_u_fin = int(getattr(spec, 'last_attempted_u', 0))
                    unique_done_fin = (attempted_u_fin >= (spec.total_target or attempted_u_fin))
                    # Adaptive retry if threshold exceeded AND unique is not already complete. IMPORTANT: do not block the loop.
                    need_retry = (rate >= float(args.retry_threshold)) and (not unique_done_fin)
                    if need_retry and int(getattr(spec, 'retry_passes', 0)) < int(args.max_retry_passes):
                        # Bump retry pass count and schedule retry occupying the same slot
                        try:
                            spec.retry_passes = int(getattr(spec, 'retry_passes', 0)) + 1  # type: ignore[attr-defined]
                        except Exception:
                            pass
                        print(f"[{spec.slug}] scheduling retry with lower concurrency", flush=True)
                        # Use per-slug retry overrides if present
                        eff_r_conc = int(_eff(spec.slug, 'retry_per_concurrency', int(args.retry_per_concurrency)))
                        eff_r_wait = float(_eff(spec.slug, 'retry_wait', float(args.retry_wait)))
                        retry_cmd = [
                            str(venv_python), "-m", "scraper.fast_scraper.crawl_external_pdfs",
                            "--edm", str(edm.resolve()),
                            "--collections", str(cols.resolve()),
                            "--out", str(spec.shard_path.resolve()),
                            "--only-collections", spec.slug,
                            "--concurrency", str(eff_r_conc),
                            "--wait", str(eff_r_wait),
                            "--timeout", str(float(args.timeout)),
                            "--retries", "1",
                            "--resume",
                            "--log-level", "WARNING",
                            "--checkpoint-every", "1000",
                            "--checkpoint-seconds", "10",
                            "--metrics-chunk", "250",
                            "--metrics-out", str(spec.metrics_path.resolve()),
                        ]
                        if args.baseline:
                            retry_cmd.extend(["--resume-baseline", str(Path(args.baseline).resolve())])
                        if bool(args.force_http1):
                            retry_cmd.append("--force-http1")
                        if str(args.ua_mode)=="random":
                            retry_cmd.append("--random-user-agent")
                        elif str(args.ua_mode)=="per_domain":
                            retry_cmd.append("--per-domain-user-agent")
                        # Replace finished process with retry in the same slot to preserve pool size.
                        with open(spec.log_path, 'ab', buffering=0) as lf:
                            retry_proc = subprocess.Popen(retry_cmd, stdout=lf, stderr=subprocess.STDOUT)
                        alive[pid] = (retry_proc, spec)
                        continue
                    # No retry: free the slot
                    del alive[pid]
                    # Rolling replenish: immediately launch the next slug when a slot frees up
                    if bool(args.rolling) and next_idx < len(slugs):
                        s2 = slugs[next_idx]
                        next_idx += 1
                        t2 = ts()
                        shard2 = shards_dir / f"external_pdfs__{s2}__{t2}.parquet"
                        metrics2 = Path("logs") / f"metrics_{s2}_{t2}.csv"
                        total2 = int(residual_by_slug.get(s2, tgt_by_slug.get(s2, 0)))
                        eff2_conc = int(_eff(s2, 'per_concurrency', int(args.per_concurrency)))
                        eff2_wait = float(_eff(s2, 'wait', float(args.wait)))
                        eff2_timeout = float(_eff(s2, 'timeout', float(args.timeout)))
                        eff2_retries = int(_eff(s2, 'retries', int(args.retries)))
                        eff2_force_h1 = bool(_eff(s2, 'force_http1', bool(args.force_http1)))
                        eff2_ua = str(_eff(s2, 'ua_mode', str(args.ua_mode)))
                        eff2_limit = int(_eff(s2, 'limit', int(args.limit))) if hasattr(args, 'limit') else int(args.limit)
                        p2 = launch_child(
                            venv_python, edm, cols, shard2, metrics2, s2,
                            concurrency=eff2_conc, wait=eff2_wait, retries=eff2_retries, child_timeout_s=eff2_timeout,
                            force_http1=eff2_force_h1, ua_mode=eff2_ua, limit=eff2_limit, baseline=Path(args.baseline) if args.baseline else None,
                        )
                        now2 = time.perf_counter()
                        spec2 = ChildSpec(
                            slug=s2,
                            shard_path=shard2,
                            log_path=Path("logs")/f"parallel_{s2}_{t2}.log",
                            metrics_path=metrics2,
                            total_target=total2,
                            start_ts=now2,
                            last_attempted=0,
                            last_ts=now2,
                            stage=1,
                            last_attempted_at_stage=0,
                            last_m_ts=now2,
                        )
                        alive[id(p2)] = (p2, spec2)
                        wave_specs.append(spec2)
                    continue
                # still running: compute minimal status from metrics first, fall back to shard (unique URLs)
                m_completed, m_eta, m_speed_ps, _m_rem = read_metrics_snapshot(spec.metrics_path)
                # Throttle shard parquet reads to reduce IO
                now = time.perf_counter()
                try:
                    parquet_refresh_s = float(os.environ.get("PARQUET_REFRESH_S", "15"))
                except Exception:
                    parquet_refresh_s = 15.0
                need_refresh = (now - float(getattr(spec, 'last_parquet_ts', 0.0))) >= parquet_refresh_s
                if need_refresh:
                    attempted_u, ok_u, s404_u, s5xx_u, neg_u, bc_best = read_unique_breakdown(spec.shard_path)
                    try:
                        spec.last_attempted_u = int(attempted_u)  # type: ignore[attr-defined]
                        spec.last_ok_u = int(ok_u)  # type: ignore[attr-defined]
                        spec.last_s404_u = int(s404_u)  # type: ignore[attr-defined]
                        spec.last_s5xx_u = int(s5xx_u)  # type: ignore[attr-defined]
                        spec.last_neg_u = int(neg_u)  # type: ignore[attr-defined]
                        spec.last_counts_best = dict(bc_best or {})  # type: ignore[attr-defined]
                        spec.last_parquet_ts = now  # type: ignore[attr-defined]
                    except Exception:
                        pass
                else:
                    attempted_u = int(getattr(spec, 'last_attempted_u', 0))
                    ok_u = int(getattr(spec, 'last_ok_u', 0))
                    s404_u = int(getattr(spec, 'last_s404_u', 0))
                    s5xx_u = int(getattr(spec, 'last_s5xx_u', 0))
                    neg_u = int(getattr(spec, 'last_neg_u', 0))
                    bc_best = getattr(spec, 'last_counts_best', {}) or {}
                breakdown = read_status_breakdown(spec.shard_path)
                skipped_rows = int(breakdown.get(-3, 0))
                # Use unique attempted for progress/fail% to avoid mixing grains
                attempted = attempted_u
                ok = ok_u
                s404 = s404_u
                s500 = s5xx_u
                # timeouts/exceptions are represented by negatives in the unique view for failish
                timeouts = int(breakdown.get(-1, 0))
                exceptions = int(breakdown.get(-2, 0))
                # Deterministic stop: if unique work is complete AND the child reports no remaining work, stop it
                try:
                    m_remaining = int(_m_rem or 0)
                except Exception:
                    m_remaining = 0
                if (attempted_u >= (spec.total_target or attempted_u)) and (m_remaining == 0):
                    print(f"[stop] {spec.slug}: unique_done and remaining=0; terminating child.", flush=True)
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        pass
                    del alive[pid]
                    # allow rolling top-up if enabled
                    if bool(args.rolling) and next_idx < len(slugs):
                        s2 = slugs[next_idx]
                        next_idx += 1
                        t2 = ts()
                        shard2 = shards_dir / f"external_pdfs__{s2}__{t2}.parquet"
                        metrics2 = Path("logs") / f"metrics_{s2}_{t2}.csv"
                        total2 = int(residual_by_slug.get(s2, tgt_by_slug.get(s2, 0)))
                        p2 = launch_child(
                            venv_python, edm, cols, shard2, metrics2, s2,
                            concurrency=int(args.per_concurrency), wait=float(args.wait), retries=int(args.retries), child_timeout_s=float(args.timeout),
                            force_http1=bool(args.force_http1), ua_mode=str(args.ua_mode), limit=int(args.limit), baseline=Path(args.baseline) if args.baseline else None,
                        )
                        now2 = time.perf_counter()
                        spec2 = ChildSpec(
                            slug=s2,
                            shard_path=shard2,
                            log_path=Path("logs")/f"parallel_{s2}_{t2}.log",
                            metrics_path=metrics2,
                            total_target=total2,
                            start_ts=now2,
                            last_attempted=0,
                            last_ts=now2,
                            stage=1,
                            last_attempted_at_stage=0,
                            last_m_ts=now2,
                        )
                        alive[id(p2)] = (p2, spec2)
                        wave_specs.append(spec2)
                    continue
                delta_t = max(1e-6, now - spec.last_ts)
                # Windowed unique-attempt delta for streak/backoff heuristics
                d_attempt = max(0, attempted - spec.last_attempted)
                # Update EMA speed using metrics deltas for fresh movement
                try:
                    prev_m = int(getattr(spec, 'last_m_completed', 0))
                except Exception:
                    prev_m = 0
                dm = max(0, int(m_completed or 0) - prev_m)
                try:
                    prev_ts = float(getattr(spec, 'last_m_ts', spec.last_ts))
                except Exception:
                    prev_ts = spec.last_ts
                dt_m = max(1e-6, now - prev_ts)
                inst_speed = dm / dt_m
                alpha = 0.30
                spec.ema_speed = inst_speed if spec.ema_speed == 0.0 else (alpha * inst_speed + (1 - alpha) * spec.ema_speed)
                # Save metrics snapshot
                try:
                    spec.last_m_completed = int(m_completed or 0)  # type: ignore[attr-defined]
                    spec.last_m_ts = now  # type: ignore[attr-defined]
                except Exception:
                    pass
                # Compute per-child ETA with fallback
                remaining_est = max(0, (spec.total_target or attempted) - attempted)
                eta_from_speed = (remaining_est / spec.ema_speed) if spec.ema_speed > 1e-6 else 0.0
                eta_s = float(m_eta) if (m_eta and float(m_eta) > 0.0) else (eta_from_speed if eta_from_speed > 0 else float(getattr(spec, 'last_eta_s', 0.0)))
                try:
                    spec.last_eta_s = eta_s  # type: ignore[attr-defined]
                except Exception:
                    pass
                remaining = max(0, (spec.total_target or attempted) - attempted)
                g_total_rem += remaining
                g_speed += max(0.0, spec.ema_speed)
                eta_m = int(eta_s // 60); eta_x = int(eta_s % 60)
                # Render a compact progress bar (20 chars)
                bar = ''
                if spec.total_target:
                    width = 20
                    filled = int(width * attempted / max(1, spec.total_target))
                    bar = '[' + '#' * filled + '-' * (width - filled) + '] '
                # Unique attempted already excludes -3; for backoff exclude 404 but include other 4xx and negatives
                effective_attempted = attempted
                try:
                    failish = sum(int(v) for k, v in (bc_best or {}).items() if (int(k) >= 400 and int(k) != 404) or int(k) < 0)
                except Exception:
                    failish = max(0, (s500 + neg_u))
                fail_pct = (failish / effective_attempted * 100.0) if effective_attempted > 0 else 0.0
                stage_label = 'A' if spec.stage==1 else ('B' if spec.stage==2 else ('C' if spec.stage==3 else '?'))
                print(f"[{spec.slug}] {bar}{attempted}/{spec.total_target or '?'} | ok={ok} 404={s404} 5xx={s500} failish={failish} skip_rows={skipped_rows} | fail%={fail_pct:.1f} | ~{spec.ema_speed:.1f} u/s | eta {eta_m:02d}:{eta_x:02d} | stage={stage_label}", flush=True)
                spec.last_attempted = attempted
                spec.last_ts = now

                # Stage backoff logic (A->B->C) based on fail% at current stage
                # Do not relaunch stages once unique work is complete
                if attempted_u < (spec.total_target or attempted_u):
                    progressed_this_stage = max(0, effective_attempted - spec.last_attempted_at_stage)
                    if progressed_this_stage >= 300 and spec.stage == 1 and fail_pct >= 20.0:
                            print(f"[stage] {spec.slug}: A→B (conc=10 wait=0.5) fail%={fail_pct:.1f} over {progressed_this_stage} attempts", flush=True)
                            try:
                                proc.terminate(); proc.wait(timeout=5)
                            except Exception:
                                pass
                            cmd2 = [
                                str(venv_python), "-m", "scraper.fast_scraper.crawl_external_pdfs",
                                "--edm", str(edm.resolve()),
                                "--collections", str(cols.resolve()),
                                "--out", str(spec.shard_path.resolve()),
                                "--only-collections", spec.slug,
                                "--concurrency", "10",
                                "--wait", "0.5",
                                "--timeout", str(float(args.timeout)),
                                "--retries", "1",
                                "--resume",
                                "--log-level", "WARNING",
                                "--checkpoint-every", "1000",
                                "--checkpoint-seconds", "10",
                                "--metrics-chunk", "250",
                                "--metrics-out", str(spec.metrics_path.resolve()),
                            ]
                            if args.baseline:
                                cmd2.extend(["--resume-baseline", str(Path(args.baseline).resolve())])
                            if bool(args.force_http1):
                                cmd2.append("--force-http1")
                            if str(args.ua_mode)=="random":
                                cmd2.append("--random-user-agent")
                            elif str(args.ua_mode)=="per_domain":
                                cmd2.append("--per-domain-user-agent")
                            with open(spec.log_path, 'ab', buffering=0) as lf:
                                newp = subprocess.Popen(cmd2, stdout=lf, stderr=subprocess.STDOUT)
                            alive[pid] = (newp, spec)
                            spec.stage = 2
                            spec.last_attempted_at_stage = effective_attempted
                    if progressed_this_stage >= 300 and spec.stage == 2 and fail_pct >= 30.0:
                        print(f"[stage] {spec.slug}: B→C (conc=5 wait=1.0) fail%={fail_pct:.1f} over {progressed_this_stage} attempts", flush=True)
                        try:
                            proc.terminate(); proc.wait(timeout=5)
                        except Exception:
                            pass
                        cmd3 = [
                            str(venv_python), "-m", "scraper.fast_scraper.crawl_external_pdfs",
                            "--edm", str(edm.resolve()),
                            "--collections", str(cols.resolve()),
                            "--out", str(spec.shard_path.resolve()),
                            "--only-collections", spec.slug,
                            "--concurrency", "5",
                            "--wait", "1.0",
                            "--timeout", str(float(args.timeout)),
                            "--retries", "1",
                            "--resume",
                            "--log-level", "WARNING",
                            "--checkpoint-every", "1000",
                            "--checkpoint-seconds", "10",
                            "--metrics-chunk", "250",
                            "--metrics-out", str(spec.metrics_path.resolve()),
                        ]
                        if args.baseline:
                            cmd3.extend(["--resume-baseline", str(Path(args.baseline).resolve())])
                        if bool(args.force_http1):
                            cmd3.append("--force-http1")
                        if str(args.ua_mode)=="random":
                            cmd3.append("--random-user-agent")
                        elif str(args.ua_mode)=="per_domain":
                            cmd3.append("--per-domain-user-agent")
                        with open(spec.log_path, 'ab', buffering=0) as lf:
                            newp = subprocess.Popen(cmd3, stdout=lf, stderr=subprocess.STDOUT)
                        alive[pid] = (newp, spec)
                        spec.stage = 3
                        spec.last_attempted_at_stage = effective_attempted

                # Decision logic: detect long negative streaks using deltas (windowed since last check)
                if attempted > 0 and d_attempt >= 200:
                    ok_delta = max(0, ok - spec.last_ok)
                    try:
                        failish_now = sum(int(v) for k, v in (bc_best or {}).items() if (int(k) >= 400 and int(k) != 404) or int(k) < 0)
                    except Exception:
                        failish_now = s500 + neg_u
                    failish_delta = max(0, failish_now - spec.last_failish)
                    if ok_delta == 0 and (failish_delta / max(1, d_attempt)) >= 0.95:
                        spec.neg_streak += d_attempt
                    else:
                        spec.neg_streak = 0
                    spec.last_ok = ok
                    spec.last_failish = failish_now
                # If streak over threshold, act
                if spec.neg_streak >= 1000 and not spec.lowered_once and not spec.dropped:
                    print(f"[{spec.slug}] high failure streak detected ({spec.neg_streak}). Lowering concurrency and continuing with resume.", flush=True)
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        pass
                    # Relaunch with low concurrency and resume
                    retry_cmd = [
                        str(venv_python), "-m", "scraper.fast_scraper.crawl_external_pdfs",
                        "--edm", str(edm.resolve()),
                        "--collections", str(cols.resolve()),
                        "--out", str(spec.shard_path.resolve()),
                        "--only-collections", spec.slug,
                        "--concurrency", "2",
                        "--wait", "1.0",
                        "--timeout", "3.0",
                        "--retries", "1",
                        "--resume",
                        "--log-level", "WARNING",
                        "--checkpoint-every", "1000",
                        "--checkpoint-seconds", "10",
                        "--metrics-chunk", "250",
                        "--metrics-out", str(spec.metrics_path.resolve()),
                    ]
                    if args.baseline:
                        retry_cmd.extend(["--resume-baseline", str(Path(args.baseline).resolve())])
                    if bool(args.force_http1):
                        retry_cmd.append("--force-http1")
                    if str(args.ua_mode)=="random":
                        retry_cmd.append("--random-user-agent")
                    elif str(args.ua_mode)=="per_domain":
                        retry_cmd.append("--per-domain-user-agent")
                    # Replace process in map with the retry process
                    with open(spec.log_path, 'ab', buffering=0) as lf:
                        new_proc = subprocess.Popen(retry_cmd, stdout=lf, stderr=subprocess.STDOUT)
                    alive[pid] = (new_proc, spec)
                    spec.lowered_once = True
                    spec.neg_streak = 0
                elif spec.neg_streak >= 1000 and spec.lowered_once and not spec.dropped:
                    print(f"[{spec.slug}] persistent failures after backoff; dropping this collection for now.", flush=True)
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=3)
                    except Exception:
                        pass
                    spec.dropped = True
                    del alive[pid]
                    # Keep pool full in rolling mode after drop
                    if bool(args.rolling) and next_idx < len(slugs):
                        s2 = slugs[next_idx]
                        next_idx += 1
                        t2 = ts()
                        shard2 = shards_dir / f"external_pdfs__{s2}__{t2}.parquet"
                        metrics2 = Path("logs") / f"metrics_{s2}_{t2}.csv"
                        total2 = int(residual_by_slug.get(s2, tgt_by_slug.get(s2, 0)))
                        p2 = launch_child(
                            venv_python, edm, cols, shard2, metrics2, s2,
                            concurrency=int(args.per_concurrency), wait=float(args.wait), retries=int(args.retries), child_timeout_s=float(args.timeout),
                            force_http1=bool(args.force_http1), ua_mode=str(args.ua_mode), limit=int(args.limit), baseline=Path(args.baseline) if args.baseline else None,
                        )
                        now2 = time.perf_counter()
                        spec2 = ChildSpec(
                            slug=s2,
                            shard_path=shard2,
                            log_path=Path("logs")/f"parallel_{s2}_{t2}.log",
                            metrics_path=metrics2,
                            total_target=total2,
                            start_ts=now2,
                            last_attempted=0,
                            last_ts=now2,
                            stage=1,
                            last_attempted_at_stage=0,
                            last_m_ts=now2,
                        )
                        alive[id(p2)] = (p2, spec2)
                        wave_specs.append(spec2)
            # Fleet/global ETA including queued slugs (rolling)
            queued_remaining = 0
            if bool(args.rolling) and next_idx < len(slugs):
                try:
                    for s in slugs[next_idx:]:
                        queued_remaining += int(residual_by_slug.get(s, tgt_by_slug.get(s, 0)))
                except Exception:
                    queued_remaining = 0
            total_remaining = int(g_total_rem) + int(queued_remaining)
            fleet_speed = max(0.0, g_speed)
            fleet_eta = (total_remaining / fleet_speed) if fleet_speed > 1e-6 else 0.0
            fm = int(fleet_eta // 60); fx = int(fleet_eta % 60)
            print(f"[fleet] alive={len(alive)} rem_alive={int(g_total_rem)} rem_queued={queued_remaining} speed~{fleet_speed:.1f} u/s | ETA {fm:02d}:{fx:02d}", flush=True)
            time.sleep(status_interval)

        # After wave completes, wait any remaining child and record its shard
        for proc, spec in procs:
            if proc.returncode is None:
                rc = proc.wait()
                print(f"[{spec.slug}] finished rc={rc} shard={spec.shard_path}", flush=True)
                rate, bc = compute_error_rate(spec.shard_path)
                print(f"[{spec.slug}] error%={rate:.2f} breakdown={bc}", flush=True)
            if spec.shard_path not in done_shards:
                done_shards.append(spec.shard_path)

        # Wave summary and optional merge after each wave to keep master fresh
        print("[wave] summary:", flush=True)
        wave_added_200 = 0
        # In rolling mode, also include any specs launched after the initial wave
        for spec in wave_specs:
            try:
                # Use unique-URL breakdown for clearer per-collection summary
                attempted_u, ok_u, s404_u, s5xx_u, neg_u, _ = read_unique_breakdown(spec.shard_path)
                print(f"  - {spec.slug}: attempted_u={attempted_u} ok={ok_u} 404={s404_u} 5xx={s5xx_u} neg={neg_u}", flush=True)
                wave_added_200 += ok_u
            except Exception:
                print(f"  - {spec.slug}: (unreadable shard)", flush=True)
        if args.merge_each_wave:
            print("[merge] merging wave shards into master...", flush=True)
            merge_master(master_out, done_shards)
            print(f"[merge] done -> {master_out}", flush=True)
        try:
            total_added_200 += wave_added_200
        except NameError:
            total_added_200 = wave_added_200

        # Advance index: in rolling mode we may have consumed beyond the initial wave
        if bool(args.rolling):
            i = next_idx
        else:
            i += int(args.max_parallel)

    # Final merge of all shards produced by this run
    print("[merge] final merge of shards into master...", flush=True)
    merge_master(master_out, done_shards)
    print(f"[merge] done -> {master_out}", flush=True)
    elapsed = time.perf_counter() - run_started
    try:
        print(f"[run] done elapsed_s={elapsed:.1f} added_200={total_added_200} orphan_processes=0", flush=True)
    except NameError:
        print(f"[run] done elapsed_s={elapsed:.1f} added_200=0 orphan_processes=0", flush=True)


if __name__ == "__main__":
    main()
