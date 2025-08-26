import argparse
import json
from pathlib import Path

import pandas as pd


def compute_stats(df: pd.DataFrame, threshold: float) -> dict:
    # Use only required columns
    df = df[["collection_slug", "language_code"]].copy()

    grouped = df.groupby("collection_slug")
    total = grouped.size().rename("total").to_frame()

    ell = (grouped["language_code"].apply(lambda s: (s == "ELL").sum()))
    none = (grouped["language_code"].apply(lambda s: (s == "NONE").sum()))

    total["ELL"] = ell
    total["NONE"] = none
    total["pct_ELL"] = (total["ELL"] / total["total"]).fillna(0.0)
    total["pct_NONE"] = (total["NONE"] / total["total"]).fillna(0.0)
    total["ELL_high"] = total["pct_ELL"] >= threshold
    total["NONE_high"] = total["pct_NONE"] >= threshold

    # Summaries
    num_ell_high = int(total["ELL_high"].sum())
    num_none_high = int(total["NONE_high"].sum())

    # Lists of collections meeting criteria
    ell_high = (
        total[total["ELL_high"]]
        .sort_values(["pct_ELL", "total"], ascending=[False, False])
        .reset_index()[["collection_slug", "total", "ELL", "pct_ELL"]]
    )
    none_high = (
        total[total["NONE_high"]]
        .sort_values(["pct_NONE", "total"], ascending=[False, False])
        .reset_index()[["collection_slug", "total", "NONE", "pct_NONE"]]
    )

    # Make JSON serializable records
    ell_high_records = [
        {
            "collection_slug": r["collection_slug"],
            "total": int(r["total"]),
            "ELL": int(r["ELL"]),
            "pct_ELL": float(r["pct_ELL"]),
        }
        for _, r in ell_high.iterrows()
    ]
    none_high_records = [
        {
            "collection_slug": r["collection_slug"],
            "total": int(r["total"]),
            "NONE": int(r["NONE"]),
            "pct_NONE": float(r["pct_NONE"]),
        }
        for _, r in none_high.iterrows()
    ]

    return {
        "total_collections": int(total.shape[0]),
        "threshold": float(threshold),
        "num_ell_high": num_ell_high,
        "num_none_high": num_none_high,
        "ell_high_collections": ell_high_records,
        "none_high_collections": none_high_records,
    }


def main():
    ap = argparse.ArgumentParser(description="Per-collection language distribution stats")
    ap.add_argument(
        "--in-parquet",
        default="openarchives_results/edm_metadata_labeled.parquet",
        help="Path to labeled Parquet with language_code",
    )
    ap.add_argument(
        "--out-json",
        default="analysis/languages/collection_language_stats.json",
        help="Output JSON path",
    )
    ap.add_argument(
        "--threshold",
        type=float,
        default=0.8,
        help="Threshold for 'high percentage' (e.g., 0.8 = 80%)",
    )
    args = ap.parse_args()

    df = pd.read_parquet(args.in_parquet, engine="pyarrow", columns=["collection_slug", "language_code"])
    stats = compute_stats(df, args.threshold)

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()







