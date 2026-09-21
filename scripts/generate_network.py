#!/usr/bin/env python3

import csv
import random
from datetime import datetime, timedelta
from ipaddress import IPv4Address


NUM_ROWS = 100_000
OUTPUT_FILE = "network_traffic.csv"

# Start of simulated capture
START_TIME = datetime(2026, 9, 21, 10, 0, 0)


def random_private_ip():
    network = random.choice([
        "10.0.0.0",
        "172.16.0.0",
        "192.168.1.0",
    ])

    return str(
        IPv4Address(network) + random.randint(1, 254)
    )


def random_public_ip():
    while True:
        ip = IPv4Address(random.randint(1, 0xFFFFFFFF))

        if not (
            ip.is_private
            or ip.is_loopback
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
            or ip.is_link_local
        ):
            return str(ip)


def random_ip():
    # 75% private, 25% public
    if random.random() < 0.75:
        return random_private_ip()

    return random_public_ip()


def random_protocol():
    return random.choices(
        ["TCP", "UDP", "ICMP"],
        weights=[65, 30, 5],
        k=1
    )[0]


def random_size(protocol):
    if protocol == "TCP":
        return random.randint(40, 1500)

    if protocol == "UDP":
        return random.randint(40, 1200)

    return random.randint(64, 512)


def generate_csv():
    current_time = START_TIME

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "time",
            "ip_src",
            "ip_dst",
            "protocol",
            "size"
        ])

        for _ in range(NUM_ROWS):
            # Advance simulated time by 1–500 microseconds
            current_time += timedelta(
                microseconds=random.randint(1, 500)
            )

            ip_src = random_ip()
            ip_dst = random_ip()

            while ip_src == ip_dst:
                ip_dst = random_ip()

            protocol = random_protocol()
            size = random_size(protocol)

            # Unix timestamp in microseconds
            timestamp = int(current_time.timestamp() * 1_000_000)

            writer.writerow([
                timestamp,
                ip_src,
                ip_dst,
                protocol,
                size
            ])

    print(f"Generated {NUM_ROWS:,} rows.")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_csv()