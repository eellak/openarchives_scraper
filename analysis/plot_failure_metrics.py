#!/usr/bin/env python3
"""Plot failure metrics and produce projections.

Inputs: logs/edm_meta_metrics_*.csv (from fetch_edm_metadata --metrics-chunk)
Outputs:
- analysis/plots/failure_rate.png
- analysis/plots/failure_derivative.png
- Printed summary with current and projected fail counts
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt


def main() -> None:
    p = argparse.ArgumentParser(description="Plot failure rate metrics and projections")
    p.add_argument("--metrics", required=True, help="Path to metrics CSV produced by fetch_edm_metadata")
    p.add_argument("--out-dir", default="analysis/plots", help="Output directory for plots")
    args = p.parse_args()

    metrics_path = Path(args.metrics)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(metrics_path)
    if df.empty:
        print({"error": "metrics CSV empty", "path": str(metrics_path)})
        sys.exit(1)

    # Basic sanity
    required = {"completed", "failed", "fail_pct", "delta_fail_pct_per_chunk", "derivative_fail_pct_per_request", "speed_ps"}
    missing = required.difference(df.columns)
    if missing:
        print({"error": "missing columns", "missing": sorted(missing)})
        sys.exit(1)

    # Plot fail% over completed
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(df["completed"], df["fail_pct"], label="fail %")
    ax.set_xlabel("Completed requests")
    ax.set_ylabel("Failure %")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fail_rate_png = out_dir / "failure_rate.png"
    fig.savefig(fail_rate_png, dpi=130)
    plt.close(fig)

    # Plot derivative per request
    fig2, ax2 = plt.subplots(figsize=(8, 4))
    ax2.plot(df["completed"], df["derivative_fail_pct_per_request"], label="d(fail%)/d(request)")
    ax2.set_xlabel("Completed requests")
    ax2.set_ylabel("Derivative (pct per request)")
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    fig2.tight_layout()
    deriv_png = out_dir / "failure_derivative.png"
    fig2.savefig(deriv_png, dpi=130)
    plt.close(fig2)

    # Projection summary
    last = df.iloc[-1]
    # Best-effort project_total (from CSV or None)
    proj_total = None
    if "project_total" in df.columns and not df["project_total"].dropna().empty:
        try:
            proj_total = int(df["project_total"].dropna().iloc[-1])
        except Exception:
            proj_total = None

    # Moving-average projection using recent chunk failure rate
    window = min(10, len(df))
    recent = df.tail(window)
    try:
        d_failed = recent["failed"].iloc[-1] - recent["failed"].iloc[0]
        d_completed = recent["completed"].iloc[-1] - recent["completed"].iloc[0]
        chunk_fail_rate = (float(d_failed) / float(max(1, d_completed))) if d_completed else 0.0
    except Exception:
        chunk_fail_rate = 0.0
    current_failed = int(last.get("failed", 0))
    current_completed = int(last.get("completed", 0))
    proj_fail_count_mv = None
    proj_fail_pct_mv = None
    if proj_total and proj_total > current_completed:
        remaining = proj_total - current_completed
        proj_fail_count_mv = int(round(current_failed + chunk_fail_rate * remaining))
        proj_fail_pct_mv = float(proj_fail_count_mv) / float(proj_total) * 100.0

    # Also surface any in-file projected fields (if present)
    proj_fail_pct = last.get("project_fail_pct") if "project_fail_pct" in df.columns else None
    proj_fail_count = last.get("project_fail_count") if "project_fail_count" in df.columns else None

    summary = {
        "metrics_csv": str(metrics_path),
        "plots": {
            "failure_rate": str(fail_rate_png),
            "failure_derivative": str(deriv_png),
        },
        "current": {
            "completed": int(last["completed"]),
            "failed": int(last["failed"]),
            "fail_pct": float(last["fail_pct"]),
            "speed_ps": float(last["speed_ps"]),
        },
        "projection": {
            "project_total": int(proj_total) if proj_total is not None else None,
            "project_fail_pct_mv": round(float(proj_fail_pct_mv), 6) if proj_fail_pct_mv is not None else None,
            "project_fail_count_mv": int(proj_fail_count_mv) if proj_fail_count_mv is not None else None,
            "project_fail_pct_file": float(proj_fail_pct) if proj_fail_pct is not None and pd.notna(proj_fail_pct) else None,
            "project_fail_count_file": int(proj_fail_count) if proj_fail_count is not None and pd.notna(proj_fail_count) else None,
            "chunk_fail_rate_recent": round(chunk_fail_rate, 6),
        },
    }
    print(summary)
    # Persist JSON summary alongside plots
    import json
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
