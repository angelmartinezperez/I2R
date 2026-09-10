"""
IDEA: A simple data structure that holds the top k elements with a fixed array size. It is not efficient.
COULD AFFECT RESULTS: the string comparison is > (not hashed), that may bias the results in case of collisions in all buckets.
PROMPT: I want to change the implementation to something simpler. As we know the elements can only increase in increments of 1, I want to create an array of k elements, each element being a tuple (count, key). Comparison between these tuples should prioritise count and then compare by key (larger first too so that the empty strings are removed).
The array is always ordered. It is initialized to all elements (0, ""). When an element is updated, it is searched in the array and moved left until its position is correct. If it is not found, proceed as if it were in the position k + 1.
"""
class TopKArray:
    """
    Fixed-size array holding the top-k (count, key) pairs, kept sorted
    in descending order.

    Ordering: count first (larger is "greater"), then key (larger is
    "greater"). Since "" is the smallest string, placeholder entries
    (0, "") naturally fall to the end of the array and are the first
    to be evicted.

    The array is always sorted. When an element is updated:
      - if it is already present, its count is incremented and it is
        bubbled left (counts only grow, so it can only move toward
        index 0);
      - if it is absent, it is inserted as (1, key) starting from a
        conceptual position just past the end (index k), shifting
        smaller entries right. If it never beats the last element, it
        falls off the array.
    """

    def __init__(self, k: int):
        self.k = k
        self.arr = [(0, "")] * k      # always sorted, descending
        self.total_count = 0          # total number of update() calls

    def _find(self, key: str) -> int:
        for i in range(self.k):
            if self.arr[i][1] == key:
                return i
        return -1

    def update(self, key: str, count: int) -> None:
        self.total_count += 1

        idx = self._find(key)
        if idx >= 0:
            # count is monotonic non-decreasing, so the key can only move left
            self.arr[idx] = (count, key)
            while idx > 0 and self.arr[idx] > self.arr[idx - 1]:
                self.arr[idx], self.arr[idx - 1] = self.arr[idx - 1], self.arr[idx]
                idx -= 1
        else:
            elem = (count, key)
            i = self.k - 1
            while i >= 0 and elem > self.arr[i]:
                if i + 1 < self.k:
                    self.arr[i + 1] = self.arr[i]
                i -= 1
            if i + 1 < self.k:
                self.arr[i + 1] = elem

    def top_k(self):
        """
        Return (top_list, rest).

        top_list : [(key, count), ...] sorted descending by count
        rest     : total_count - sum of counts in top_list
        """
        top = [(key, count) for count, key in self.arr if key != ""]
        top_sum = sum(count for _, count in top)
        rest = self.total_count - top_sum
        return top, rest

    def __repr__(self):
        return f"TopKArray(k={self.k}, arr={self.arr}, total={self.total_count})"