import re

from CountMinSketch import CountMinSketch

# --- Config ---
PATH = "./test/el_quijote.txt"
HASHES = 5
BUCKETS = 100_000
K = 20

# Word tokenizer: sequences of letters (including accented chars), lowercased.
WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def tokenize(text: str):
    for match in WORD_RE.finditer(text.lower()):
        yield match.group()


def main():
    cms = CountMinSketch(hashes=HASHES, buckets=BUCKETS, k=K, seed=42)

    total = 0
    with open(PATH, "r", encoding="utf-8") as f:
        for line in f:
            for word in tokenize(line):
                cms.update(word)
                total += 1

    top, rest = cms.top_k()

    print(f"Read {total} tokens from {PATH}\n")
    print(f"Top {K} words (by Count-Min estimate):")
    for rank, (word, count) in enumerate(top, start=1):
        print(f"{rank:>3}. {word:<20} {count}")

    print(f"\nTotal tokens:  {total}")
    print(f"Top-{K} sum:    {total - rest}")
    print(f"Rest:          {rest}")


if __name__ == "__main__":
    main()