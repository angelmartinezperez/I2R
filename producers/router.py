from pathlib import Path
import csv
import sys

from kafka import KafkaProducer
from kafka.serializer.json import JsonSerializer

from CMS.CountMinSketch import CountMinSketch

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
INPUT_FILE = Path(__file__).resolve().parent / "input" / "network_traffic.csv"
TOPIC = "network_events_individual"
BOOTSTRAP_SERVERS = "broker:9092"
CHUNK_US = 100_000          # 100 ms of CSV event time (timestamps are µs)
CMS_HASHES = 5
CMS_BUCKETS = 100
CMS_K = 20

REST_IP = ""               # sentinel for the "rest" aggregate event
ROUTER_ID = 1              # constant key used for every Kafka message
KEY_SEP = "|"


def flow_key(ip_src: str, ip_dst: str) -> str:
    return f"{ip_src}{KEY_SEP}{ip_dst}"


def parse_flow_key(key: str):
    ip_src, ip_dst = key.split(KEY_SEP, 1)
    return ip_src, ip_dst


def emit_chunk(producer, cms, chunk_id: int, packet_count: int):
    top_list, rest = cms.top_k()
    event_ts_ms = (chunk_id * CHUNK_US) // 1000

    for key, count in top_list:
        ip_src, ip_dst = parse_flow_key(key)
        payload = {
            "id": ROUTER_ID,
            "timestamp": event_ts_ms,
            "ip_src": ip_src,
            "ip_dst": ip_dst,
            "count": count,
        }
        producer.send(
            TOPIC,
            value=payload,
            key=ROUTER_ID,
            timestamp_ms=event_ts_ms,
        )

    rest_payload = {
        "id": ROUTER_ID,
        "timestamp": event_ts_ms,
        "ip_src": REST_IP,
        "ip_dst": REST_IP,
        "count": rest,
    }
    producer.send(
        TOPIC,
        value=rest_payload,
        key=ROUTER_ID,
        timestamp_ms=event_ts_ms,
    )

    print(
        f"Chunk {chunk_id:>12} | {packet_count:>5} packets | "
        f"emitted {len(top_list)} top-k events + 1 rest event "
        f"(rest={rest}) @ ts_ms={event_ts_ms}"
    )


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.to_bytes(4, byteorder="big"),
        value_serializer=JsonSerializer(),
    )

    current_chunk_id = None
    cms = None
    packet_count = 0

    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            time_us = int(row["time"])
            chunk_id = time_us // CHUNK_US

            if current_chunk_id is None:
                current_chunk_id = chunk_id
                cms = CountMinSketch(
                    hashes=CMS_HASHES,
                    buckets=CMS_BUCKETS,
                    k=CMS_K,
                )
                packet_count = 0
            elif chunk_id != current_chunk_id:
                emit_chunk(producer, cms, current_chunk_id, packet_count)
                current_chunk_id = chunk_id
                cms = CountMinSketch(
                    hashes=CMS_HASHES,
                    buckets=CMS_BUCKETS,
                    k=CMS_K,
                )
                packet_count = 0

            cms.update(flow_key(row["ip_src"], row["ip_dst"]))
            packet_count += 1

    if cms is not None and packet_count > 0:
        emit_chunk(producer, cms, current_chunk_id, packet_count)

    producer.flush()
    producer.close()
    print("Done – all packets processed.")


if __name__ == "__main__":
    main()
