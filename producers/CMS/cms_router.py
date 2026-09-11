import re
import json
import time
from kafka import KafkaProducer
from CountMinSketch import CountMinSketch

# ------------------------------------------------------------
# Word tokenizer: sequences of letters (including accented chars), lowercased.
# ------------------------------------------------------------
WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def tokenize(text: str):
    for match in WORD_RE.finditer(text.lower()):
        yield match.group()


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
INPUT_FILE = "./test/el_quijote.txt"
TOPIC = "wordcount_events"
BOOTSTRAP_SERVERS = "localhost:9092"
BATCH_SIZE = 1000          # words per batch
INTERVAL_SEC = 0.1         # 100 ms
CMS_HASHES = 5
CMS_BUCKETS = 20
CMS_K = 20


def main():
    # 1. Read and tokenize the whole file
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        text = f.read()
    tokens = list(tokenize(text))
    total_tokens = len(tokens)

    # 2. Set up Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # We send raw bytes; no need for a value_serializer.
    )

    # 3. Process in batches of BATCH_SIZE, emitting every INTERVAL_SEC
    batch_start = 0
    while batch_start < total_tokens:
        t_batch_start = time.monotonic()

        # Extract the next batch (may be shorter at EOF)
        batch = tokens[batch_start : batch_start + BATCH_SIZE]

        # Reset the CMS for each batch (fresh instance)
        cms = CountMinSketch(
            hashes=CMS_HASHES,
            buckets=CMS_BUCKETS,
            k=CMS_K,
        )

        # Feed the batch
        for word in batch:
            cms.update(word)

        # Get top‑k result
        top_list, rest = cms.top_k()

        # Build payload – include explicit timestamp for Flink event time
        payload = {
            "top_k": top_list,          # list of [word, count] pairs
            "rest": rest,               # total count – sum of top_k counts
            "batch_start": batch_start, # index of first word in this batch
            "batch_size": len(batch),
            "timestamp": time.time(),   # epoch seconds (float)
        }
        value_bytes = json.dumps(payload).encode("utf-8")

        # Send synchronously (null key → round‑robin partitioning)
        future = producer.send(
            TOPIC,
            value=value_bytes,
            key=None,
            timestamp_ms=int(time.time() * 1000),
        )
        result = future.get(timeout=60)
        print(
            f"Batch {batch_start:>7} | {len(batch):>4} words | "
            f"delivered to {result.topic} [{result.partition}] @ offset {result.offset}"
        )

        # Advance batch pointer
        batch_start += BATCH_SIZE

        # Maintain the 100 ms cadence
        elapsed = time.monotonic() - t_batch_start
        sleep_time = INTERVAL_SEC - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    # 4. Clean shutdown
    producer.close()
    print("Done – all tokens processed.")


if __name__ == "__main__":
    main()