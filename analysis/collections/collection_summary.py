import argparse
from typing import List, Dict, Any

import pandas as pd


def summarize_collection(group: pd.DataFrame, top_n: int) -> Dict[str, Any]:
    total = int(len(group))
    lang = group["language_code"].fillna("")
    num_ell = int((lang == "ELL").sum())
    num_none = int((lang == "NONE").sum())
    pct_ell = float(num_ell / total) if total else 0.0
    pct_none = float(num_none / total) if total else 0.0

    types = group["type"].fillna("NA").astype(str)
    vc = types.value_counts()
    top_types = vc.head(top_n)
    types_top: List[str] = top_types.index.tolist()
    types_top_counts: List[int] = [int(x) for x in top_types.values.tolist()]
    types_top_pcts: List[float] = [float(c / total) if total else 0.0 for c in types_top_counts]

    return {
        "total_items": total,
        "num_ELL": num_ell,
        "pct_ELL": pct_ell,
        "num_NONE": num_none,
        "pct_NONE": pct_none,
        "types_top": types_top,
        "types_top_counts": types_top_counts,
        "types_top_pcts": types_top_pcts,
    }


def main():
    ap = argparse.ArgumentParser(description="Per-collection summary: totals, language %, top types")
    ap.add_argument(
        "--in-parquet",
        default="openarchives_results/edm_metadata_labeled.parquet",
        help="Input labeled parquet (must contain collection_slug, language_code, type)",
    )
    ap.add_argument(
        "--out-parquet",
        default="analysis/collections/collection_summary.parquet",
        help="Output parquet path",
    )
    ap.add_argument("--top-n", type=int, default=5, help="Top-N types to include")
    args = ap.parse_args()

    cols = ["collection_slug", "language_code", "type"]
    df = pd.read_parquet(args.in_parquet, engine="pyarrow", columns=cols)

    records: List[Dict[str, Any]] = []
    keys: List[str] = []
    for collection_slug, grp in df.groupby("collection_slug", sort=False):
        keys.append(collection_slug)
        records.append(summarize_collection(grp, args.top_n))

    out_df = pd.DataFrame(records)
    out_df.insert(0, "collection_slug", keys)

    out_df.to_parquet(args.out_parquet, engine="pyarrow", compression="snappy", index=False)
    print(f"Wrote {len(out_df)} rows to {args.out_parquet}")


if __name__ == "__main__":
    main()







