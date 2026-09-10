from CountMinSketch import CountMinSketch

cms = CountMinSketch(hashes=5, buckets=1000, k=3, seed=42)

for word in ["apple", "banana", "apple", "cherry",
             "apple", "banana", "date", "date", "date"]:
    cms.update(word)

top, rest = cms.top_k()
print(top)    # [('apple', 3), ('date', 3), ('banana', 2)]
print(rest)   # 1  (the lone "cherry" update that got pushed out)