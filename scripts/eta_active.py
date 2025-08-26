#!/usr/bin/env python3
"""Show ETA for all running collections based on metrics CSVs.

Reads logs/metrics_*.csv files that each crawler writes and reports:
- Per-collection: remaining, speed (items/s), ETA (hh:mm)
- Global: number of active collections, current-wave ETA (max per-collection ETA)

Usage examples:
  .venv/bin/python scripts/eta_active.py
  .venv/bin/python scripts/eta_active.py --dir logs --active-minutes 10
  .venv/bin/python scripts/eta_active.py --json
  .venv/bin/python scripts/eta_active.py --watch 30
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Tuple


@dataclass
class MetricSample:
    slug: str
    completed: int
    failed: int
    fail_pct: float
    speed_ps: float
    eta_s: int
    remaining: int
    file: str


def parse_slug_from_metrics_path(path: Path) -> str:
    # Expect: metrics_<slug>_<YYYYMMDD>_<HHMMSS>.csv
    stem = path.stem
    parts = stem.split("_")
    if len(parts) >= 4 and parts[0] == "metrics":
        return "_".join(parts[1:-2])
    # Fallback: best-effort (e.g., metrics_<slug>_<ts>)
    if len(parts) >= 2 and parts[0] == "metrics":
        return "_".join(parts[1:-1])
    return stem


def tail_last_row_csv(path: Path) -> Optional[List[str]]:
    """Read the last non-header CSV row using a small tail read."""
    try:
        with open(path, "rb") as fh:
            try:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                read_size = min(8192, size)
                fh.seek(-read_size, os.SEEK_END)
                chunk = fh.read(read_size)
            except OSError:
                fh.seek(0)
                chunk = fh.read()
        text = chunk.decode("utf-8", errors="ignore")
        lines = [ln for ln in text.strip().splitlines() if ln.strip()]
        if not lines:
            return None
        # Find last non-header line with at least 7 fields
        for ln in reversed(lines):
            if ln.lower().startswith("t_epoch"):
                continue
            if ln.count(",") >= 6:
                return [p.strip() for p in ln.split(",")]
        return None
    except Exception:
        return None


def collect_active_metrics(dir_path: Path, active_window_s: int) -> List[MetricSample]:
    now = time.time()
    out: List[MetricSample] = []
    for p in sorted(dir_path.glob("metrics_*.csv")):
        try:
            st = p.stat()
        except FileNotFoundError:
            continue
        if (now - st.st_mtime) > active_window_s:
            continue
        row = tail_last_row_csv(p)
        if not row or len(row) < 7:
            continue
        try:
            # header: t_epoch, completed, failed, fail_pct, speed_ps, eta_s, remaining
            slug = parse_slug_from_metrics_path(p)
            completed = int(float(row[1]))
            failed = int(float(row[2]))
            fail_pct = float(row[3])
            speed_ps = float(row[4])
            eta_s = int(float(row[5]))
            remaining = int(float(row[6]))
        except Exception:
            continue
        out.append(
            MetricSample(
                slug=slug,
                completed=completed,
                failed=failed,
                fail_pct=fail_pct,
                speed_ps=max(0.0, speed_ps),
                eta_s=max(0, eta_s),
                remaining=max(0, remaining),
                file=p.name,
            )
        )
    # Sort by ETA descending (critical path first)
    out.sort(key=lambda m: m.eta_s, reverse=True)
    return out


def fmt_eta(s: int) -> str:
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h:02d}h{m:02d}m"


def render_text(samples: List[MetricSample]) -> str:
    if not samples:
        return "no active metrics found"
    lines: List[str] = []
    max_eta = samples[0].eta_s if samples else 0
    lines.append(
        f"active={len(samples)} current_wave_eta={fmt_eta(max_eta)} critical={samples[0].slug if samples else '-'}"
    )
    lines.append("per-collection:")
    for m in samples:
        lines.append(
            f"- {m.slug}: rem={m.remaining} spd={m.speed_ps:.2f}/s eta={fmt_eta(m.eta_s)} ({m.file})"
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Show ETA for all running collections")
    ap.add_argument("--dir", default="logs", help="Directory with metrics_*.csv files")
    ap.add_argument(
        "--active-minutes",
        type=int,
        default=10,
        help="Consider metrics files updated within this many minutes as active",
    )
    ap.add_argument("--json", action="store_true", help="Output JSON instead of text")
    ap.add_argument(
        "--watch",
        type=int,
        default=0,
        help="Refresh every N seconds (0 to disable)",
    )
    args = ap.parse_args()

    dir_path = Path(args.dir)
    if not dir_path.exists():
        print(f"metrics directory not found: {dir_path}")
        raise SystemExit(2)

    def _run_once():
        samples = collect_active_metrics(dir_path, active_window_s=max(1, args.active_minutes) * 60)
        if args.json:
            payload = {
                "active": len(samples),
                "current_wave_eta_s": samples[0].eta_s if samples else 0,
                "current_wave_eta": fmt_eta(samples[0].eta_s) if samples else "00h00m",
                "critical_slug": samples[0].slug if samples else None,
                "collections": [asdict(s) for s in samples],
            }
            print(json.dumps(payload, indent=2))
        else:
            print(render_text(samples))

    if args.watch and args.watch > 0:
        try:
            while True:
                _run_once()
                time.sleep(args.watch)
        except KeyboardInterrupt:
            return
    else:
        _run_once()


if __name__ == "__main__":
    main()

