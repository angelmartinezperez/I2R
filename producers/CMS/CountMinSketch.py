# This hash is non cryptographic, that is not an issue for this use case.
# We just need a hash that can accept a seed.
import mmh3
try:
    from .TopKArray import TopKArray
except ImportError:
    from TopKArray import TopKArray

# Incredibly inefficient implementation.
# No parallelism, no efficient hash usage, I think even the memory usage is inefficient.
class CountMinSketch:
    def __init__(self, hashes: int, buckets: int, k: int, seed: int = 0):
        self.rows = hashes
        self.cols = buckets
        self.seeds = [seed + i for i in range(hashes)]
        self.count_matrix = [[0] * buckets for _ in range(hashes)]
        self.topk = TopKArray(k)

    def _bucket(self, element, row: int) -> int:
        data = element.encode() if isinstance(element, str) else element
        h = mmh3.hash(data, self.seeds[row], signed=False)
        return h % self.cols

    def update(self, element):
        for row in range(self.rows):
            idx = self._bucket(element, row)
            self.count_matrix[row][idx] += 1
        self.topk.update(element, self.query(element))

    def query(self, element) -> int:
        return min(
            self.count_matrix[row][self._bucket(element, row)]
            for row in range(self.rows)
        )

    def top_k(self):
        return self.topk.top_k()