import json, re, unicodedata
from collections import defaultdict
from pathlib import Path

# Paths
base = Path(__file__).resolve().parent.parent
inp = base / "analysis" / "type_values_full.json"
out_dir = base / "analysis"
out_dir.mkdir(parents=True, exist_ok=True)

# Load
obj = json.loads(inp.read_text(encoding="utf-8"))
vals = obj.get("values_sorted", [])

# Normalize utility
def strip_accents(text: str) -> str:
    try:
        text = unicodedata.normalize("NFD", text)
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        return unicodedata.normalize("NFC", text)
    except Exception:
        return text

norm_types = []
for v, c in vals:
    s = " ".join(str(v).split())
    s = strip_accents(s).lower()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    if s:
        norm_types.append(s)

N = len(norm_types)

# Build 6-char n-gram tokens from alphabetic words
ngram_to_idxs = defaultdict(set)
for i, s in enumerate(norm_types):
    words = s.split()
    seen = set()
    for w in words:
        w_alpha = .join(ch for ch in w if ch.isalpha())
        if len(w_alpha) < 6:
            continue
        for j in range(len(w_alpha) - 6 + 1):
            tok = w_alpha[j:j+6]
            seen.add(tok)
    for tok in seen:
        ngram_to_idxs[tok].add(i)

# Rank tokens by coverage
ranked = sorted(((t, len(idxs)) for t, idxs in ngram_to_idxs.items()), key=lambda x: (-x[1], x[0]))

# Write top tokens file
(out_dir / "type_stem6_ngrams_top.txt").write_text(
    "\n".join(f"{cnt}\t{cnt/N:.4f}\t{tok}" for tok, cnt in ranked), encoding="utf-8"
)

# Greedy set cover

def greedy_cover(target_frac: float):
    target = int(target_frac * N)
    covered = set()
    selected = []
    remaining = set(ngram_to_idxs.keys())
    while len(covered) < target and remaining:
        best_tok = None
        best_gain = -1
        best_cov = 0
        for t in list(remaining):
            idxs = ngram_to_idxs[t]
            gain = len(idxs - covered)
            if gain > best_gain or (gain == best_gain and (len(idxs) > best_cov or (len(idxs) == best_cov and (best_tok is None or t < best_tok)))):
                best_tok = t
                best_gain = gain
                best_cov = len(idxs)
        if not best_tok or best_gain <= 0:
            break
        covered |= ngram_to_idxs[best_tok]
        selected.append({
            "token": best_tok,
            "gain": best_gain,
            "token_cover_count": best_cov,
            "covered_so_far": len(covered)
        })
        remaining.remove(best_tok)
    return {
        "total_types": N,
        "target_frac": target_frac,
        "target_count": target,
        "selected_count": len(selected),
        "final_covered": len(covered),
        "final_frac": (len(covered)/N) if N else 0.0,
        "selected": selected,
        "uncovered_examples": [norm_types[i] for i in range(N) if i not in covered][:50]
    }

res80 = greedy_cover(0.80)
res90 = greedy_cover(0.90)
res95 = greedy_cover(0.95)

(out_dir / "type_stem6_ngrams_greedy_cover_0.80.json").write_text(json.dumps(res80, ensure_ascii=False, indent=2), encoding="utf-8")
(out_dir / "type_stem6_ngrams_greedy_cover_0.90.json").write_text(json.dumps(res90, ensure_ascii=False, indent=2), encoding="utf-8")
(out_dir / "type_stem6_ngrams_greedy_cover_0.95.json").write_text(json.dumps(res95, ensure_ascii=False, indent=2), encoding="utf-8")

print({
    "N_types": N,
    "num_tokens6": len(ngram_to_idxs),
    "cover80": {"selected": res80["selected_count"], "final_frac": round(res80["final_frac"], 4)},
    "cover90": {"selected": res90["selected_count"], "final_frac": round(res90["final_frac"], 4)},
    "cover95": {"selected": res95["selected_count"], "final_frac": round(res95["final_frac"], 4)}
})
