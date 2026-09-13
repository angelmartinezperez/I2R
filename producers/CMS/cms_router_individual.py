import re
import json
import time
from kafka import KafkaProducer
from kafka.serializer import JsonSerializer
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
TOPIC = "wordcount_events_individual"
BOOTSTRAP_SERVERS = "localhost:9092"
BATCH_SIZE = 1000          # words per batch
INTERVAL_SEC = 0.1         # simulated 100 ms between batches
CMS_HASHES = 5
CMS_BUCKETS = 20
CMS_K = 20

REST_WORD = ""             # sentinel word for the "rest" aggregate event
ROUTER_ID = 1              # constant key used for every Kafka message


def main():
    # 1. Read and tokenize the whole file
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        text = f.read()
    tokens = list(tokenize(text))
    total_tokens = len(tokens)

    # 2. Set up Kafka producer – JsonSerializer handles the value encoding,
    #    and keys are encoded as big-endian int bytes.
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.to_bytes(4, byteorder="big"),
        value_serializer=JsonSerializer(),
    )

    # 3. Simulated event-time clock: starts at "now", advances by
    #    INTERVAL_SEC per batch so downstream event-time processing
    #    sees a steady stream without us actually sleeping.
    simulated_ts = time.time()

    batch_start = 0
    while batch_start < total_tokens:
        batch = tokens[batch_start : batch_start + BATCH_SIZE]

        cms = CountMinSketch(
            hashes=CMS_HASHES,
            buckets=CMS_BUCKETS,
            k=CMS_K,
        )
        for word in batch:
            cms.update(word)

        top_list, rest = cms.top_k()

        # One shared timestamp for all events of this batch
        event_ts = simulated_ts
        event_ts_ms = int(event_ts * 1000)

        # Emit one Kafka message per (word, count) pair
        for word, count in top_list:
            payload = {
                "word": word,
                "count": count,
            }
            producer.send(
                TOPIC,
                value=payload,                 # dict → JSON bytes via JsonSerializer
                key=ROUTER_ID,                 # constant int key → bytes
                timestamp_ms=event_ts_ms,      # Kafka record timestamp
            )

        # Emit a separate event for the "rest" aggregate, now using the
        # same constant ROUTER_ID key so it stays on the same partition.
        rest_payload = {
            "word": REST_WORD,
            "count": rest,
        }

        producer.send(
            TOPIC,
            value=rest_payload,
            key=ROUTER_ID,
            timestamp_ms=event_ts_ms,
        )

        print(
            f"Batch {batch_start:>7} | {len(batch):>4} words | "
            f"emitted {len(top_list)} top-k events + 1 rest event "
            f"(rest={rest}) @ ts={event_ts:.3f}"
        )

        batch_start += BATCH_SIZE

        # Advance the simulated event time (no real sleep)
        simulated_ts += INTERVAL_SEC

    # 4. Flush any in-flight records and shut down
    producer.flush()
    producer.close()
    print("Done – all tokens processed.")


if __name__ == "__main__":
    main()