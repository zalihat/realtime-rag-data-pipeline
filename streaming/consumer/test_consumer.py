#!/usr/bin/env python3
"""
test_consumer.py

Minimal sanity check before running the real embed_consumer.py.
Connects to the `support.public.ticket_resolutions` topic and just
prints every message as-is -- no embedding, no DB writes. Confirms
Kafka is reachable and messages actually look like what we expect
(Debezium envelope with before/after/op).

Usage:
    python test_consumer.py

Ctrl+C to stop.
"""

import json
import os

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "support.public.ticket_resolutions")


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}, topic '{KAFKA_TOPIC}'...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="test-consumer",       # separate group id from the real consumer
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    print("Listening for messages... (Ctrl+C to stop)\n")
    try:
        for message in consumer:
            print(f"--- offset {message.offset} ---")
            print(json.dumps(message.value, indent=2))
            print()
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()