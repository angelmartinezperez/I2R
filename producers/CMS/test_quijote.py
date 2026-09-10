import re
from collections import Counter

from CountMinSketch import CountMinSketch

# --- Config ---
PATH = "./test/el_quijote.txt"
HASHES = 7
BUCKETS = 100
K = 30

# Word tokenizer: sequences of letters (including accented chars), lowercased.
WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def tokenize(text: str):
    for match in WORD_RE.finditer(text.lower()):
        yield match.group()


def main():
    cms = CountMinSketch(hashes=HASHES, buckets=BUCKETS, k=K, seed=42)
    real = Counter()   # exact counts, for comparison only

    total = 0
    with open(PATH, "r", encoding="utf-8") as f:
        for line in f:
            for word in tokenize(line):
                cms.update(word)
                real[word] += 1
                total += 1

    top, rest = cms.top_k()
    real_top = real.most_common(K)

    print(f"Read {total} tokens from {PATH}\n")

    # --- Side-by-side ranking ---
    print(f"Side-by-side top-{K} ranking")
    print(
        f"{'#':>3}  "
        f"{'sketch word':<18} {'est':>8}  "
        f"{'real word':<18} {'real':>8}"
    )
    print("-" * 70)

    for rank in range(K):
        if rank < len(top):
            s_word, s_est = top[rank]
            s_word_disp, s_est_disp = s_word, s_est
        else:
            s_word_disp, s_est_disp = "-", "-"

        if rank < len(real_top):
            r_word, r_count = real_top[rank]
            r_word_disp, r_count_disp = r_word, r_count
        else:
            r_word_disp, r_count_disp = "-", "-"

        print(
            f"{rank + 1:>3}. "
            f"{s_word_disp:<18} {str(s_est_disp):>8}  "
            f"{r_word_disp:<18} {str(r_count_disp):>8}"
        )

    # --- Set-based metrics (ignore order) ---
    sketch_set = {w for w, _ in top}
    real_set = {w for w, _ in real_top}

    true_positives = sketch_set & real_set

    precision = len(true_positives) / len(sketch_set) if sketch_set else 0.0
    recall = len(true_positives) / len(real_set) if real_set else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0 else 0.0
    )

    print("-" * 70)
    print(f"Words in both top-{K} rankings: {len(true_positives)}")
    print(f"  Precision: {precision:.3f}  ({len(true_positives)}/{len(sketch_set)})")
    print(f"  Recall:    {recall:.3f}  ({len(true_positives)}/{len(real_set)})")
    print(f"  F1:        {f1:.3f}")

    missing = real_set - sketch_set
    extra = sketch_set - real_set
    if missing:
        print(f"\nIn real top-{K} but missing from sketch top-{K} ({len(missing)}):")
        print("  " + ", ".join(sorted(missing)))
    if extra:
        print(f"\nIn sketch top-{K} but not in real top-{K} ({len(extra)}):")
        print("  " + ", ".join(sorted(extra)))

    # --- Detail table: sketch top-k with real counts ---
    print(f"\nTop {K} words as chosen by the sketch (estimate vs. real):")
    print(f"{'#':>3}  {'word':<20} {'estimate':>10}  {'real':>10}")
    for rank, (word, est) in enumerate(top, start=1):
        print(f"{rank:>3}. {word:<20} {est:>10}  {real[word]:>10}")

    top_est_sum = total - rest
    top_real_sum = sum(real[w] for w, _ in top)

    print(f"\nTotal tokens:        {total}")
    print(f"Top-{K} sum (est):    {top_est_sum}")
    print(f"Top-{K} sum (real):   {top_real_sum}")
    print(f"Rest (est):          {rest}")
    print(f"Rest (real):         {total - top_real_sum}")


if __name__ == "__main__":
    main()