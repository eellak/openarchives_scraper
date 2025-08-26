from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


def load_language_rules(path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        if "->" not in raw:
            continue
        lhs, rhs = raw.split("->", 1)
        lhs = lhs.strip()
        rhs = rhs.strip()
        # Support extra note after code (e.g., "ELL [αρχαία]")
        code = rhs.split()[0]
        mapping[lhs] = code
    return mapping


def load_type_rules(path: Path) -> Dict[str, Tuple[str, List[str]]]:
    """Return mapping: token_lhs -> (label_rhs, covered_values[])"""
    mapping: Dict[str, Tuple[str, List[str]]] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        # Strip leading notes like [[X]] [[D]] while preserving rest
        while line.startswith("[["):
            end = line.find("]] ")
            if end == -1:
                break
            line = line[end + 3:]
        if "->" not in line:
            continue
        left, right = line.split("->", 1)
        lhs = left.strip()
        label = right.strip()
        values: List[str] = []
        if " : " in right:
            label_part, json_part = right.split(" : ", 1)
            label = label_part.strip()
            try:
                values = json.loads(json_part.strip())
                if not isinstance(values, list):
                    values = [str(values)]
            except Exception:
                values = []
        mapping[lhs] = (label, values)
    return mapping


def strip_accents(text: str) -> str:
    import unicodedata
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", text)


def normalize(s: str) -> str:
    return strip_accents((s or "").strip().lower())


def build_language_maps(lang_rules: Dict[str, str]) -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    exact: Dict[str, str] = {}
    contains: List[Tuple[str, str]] = []
    for lhs, code in lang_rules.items():
        n = normalize(lhs)
        if n:
            exact[n] = code
            contains.append((n, code))
    # sort longer first for contains matching
    contains.sort(key=lambda x: -len(x[0]))
    return exact, contains


def detect_language_code(meta: Dict[str, str], lang_exact: Dict[str, str], lang_contains: List[Tuple[str, str]]) -> str:
    raw = meta.get("Γλώσσα") or meta.get("Language") or meta.get("language") or ""
    key = normalize(raw)
    if not key:
        return "NONE"  # absence mapping requested
    code = lang_exact.get(key)
    if code:
        return code
    for tok, code in lang_contains:
        if tok and tok in key:
            return code
    return "OTHER"


def build_type_value_to_label(type_rules: Dict[str, Tuple[str, List[str]]]) -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    exact: Dict[str, str] = {}
    contains: List[Tuple[str, str]] = []
    for _lhs, (label, covered) in type_rules.items():
        for v in covered:
            nv = normalize(v)
            if not nv:
                continue
            # prefer first mapping if duplicates differ
            exact.setdefault(nv, label)
            contains.append((nv, label))
    # sort longer first to match more specific strings first
    contains.sort(key=lambda x: -len(x[0]))
    return exact, contains


def detect_type_label(raw: str, type_exact: Dict[str, str], type_contains: List[Tuple[str, str]]) -> str:
    raw_n = normalize(raw)
    if not raw_n:
        return "NA"
    label = type_exact.get(raw_n)
    if label:
        return label
    for tok, lab in type_contains:
        if tok and (tok in raw_n or raw_n in tok):
            return lab
    return "NA"


def title_all_english(meta: Dict[str, str]) -> bool:
    title = meta.get("Τίτλος") or meta.get("Title") or meta.get("title") or ""
    if not title:
        return False
    for ch in title:
        if ch.isalpha():
            # must be ASCII letter
            if not ("A" <= ch <= "Z" or "a" <= ch <= "z"):
                return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser(description="Apply language and type rules to EDM metadata Parquet.")
    ap.add_argument("--in-parquet", default="openarchives_results/edm_metadata_full.parquet")
    ap.add_argument("--out-parquet", default="openarchives_results/edm_metadata_labeled.parquet")
    ap.add_argument("--lang-rules", default="analysis/languages/language_rules.txt")
    ap.add_argument("--type-rules", default="analysis/types/type_rules_final.txt")
    args = ap.parse_args()

    lang_rules = load_language_rules(Path(args.lang_rules))
    type_rules = load_type_rules(Path(args.type_rules))

    # Build fast lookup maps
    lang_exact, lang_contains = build_language_maps(lang_rules)
    type_exact, type_contains = build_type_value_to_label(type_rules)

    df = pd.read_parquet(args.in_parquet, engine="pyarrow")

    # Fast metadata parsing
    def safe_load(s: Optional[str]) -> Dict[str, str]:
        if isinstance(s, str) and s:
            try:
                return json.loads(s)
            except Exception:
                return {}
        return {}

    metas: List[Dict[str, str]] = [safe_load(s) for s in df["metadata_json"].values]

    # Compute outputs in one pass using comprehensions
    lang_codes = [
        detect_language_code(meta, lang_exact, lang_contains) for meta in metas
    ]
    type_labels = [
        detect_type_label((meta.get("Τύπος") or meta.get("Type") or meta.get("type") or ""), type_exact, type_contains)
        for meta in metas
    ]
    titles_en = [title_all_english(meta) for meta in metas]

    df["language_code"] = lang_codes
    df["type"] = type_labels
    df["title_all_english"] = titles_en

    df.to_parquet(args.out_parquet, index=False)

    # Prepare JSON-serializable summary
    lang_counts_ser = pd.Series(lang_codes).value_counts()
    type_counts_ser = pd.Series(type_labels).value_counts().head(20)
    lang_counts = {str(k): int(v) for k, v in lang_counts_ser.items()}
    type_counts_top = {str(k): int(v) for k, v in type_counts_ser.items()}

    print(json.dumps({
        "input": args.in_parquet,
        "output": args.out_parquet,
        "rows": int(len(df)),
        "language_counts": lang_counts,
        "type_counts_top": type_counts_top,
        "title_all_english_true": int(pd.Series(titles_en).sum()),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


