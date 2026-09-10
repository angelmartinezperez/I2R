import re
from collections import Counter

from CountMinSketch import CountMinSketch

# --- Config ---
PATH = "./test/el_quijote.txt"
HASHES = 5
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

    print(f"Read {total} tokens from {PATH}\n")
    print(f"Top {K} words (Count-Min estimate vs. real count):")
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
    print(f"\nTop {K} words by real count (for reference):")
    for rank, (word, count) in enumerate(real.most_common(K), start=1):
        print(f"{rank:>3}. {word:<20} {count:>10}")


if __name__ == "__main__":
    main()