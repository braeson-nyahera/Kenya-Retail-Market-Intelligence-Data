"""Simple POS producer using kafka-python for Apache Kafka:
reads historical_pos_lines.csv and sends one Kafka message per receipt.

Usage:
  python src/pos_simulation/pos_producer.py --dry-run --limit 3      # print only, no Kafka needed
  python src/pos_simulation/pos_producer.py --limit 100              # send 100 receipts to Apache Kafka
  python src/pos_simulation/pos_producer.py --delay 0.01             # send at ~100 receipts/second
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

Producer = KafkaProducer

TOPIC = "pos.transactions"
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BOOTSTRAP") or "localhost:9092"

ap = argparse.ArgumentParser(description="POS Transaction Producer for Apache Kafka")
ap.add_argument("--source", default="data/synthetic/historical_pos_lines.csv", help="Path to historical POS CSV")
ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server (default: %(default)s)")
ap.add_argument("--delay", type=float, default=0, help="seconds to wait between receipts")
ap.add_argument("--limit", type=int, help="stop after N receipts")
ap.add_argument("--dry-run", action="store_true", help="print JSON without sending to Kafka")
args = ap.parse_args()

source_path = Path(args.source)
if not source_path.exists():
    repo_root = Path(__file__).resolve().parent.parent.parent
    candidate = repo_root / args.source
    if candidate.exists():
        source_path = candidate
    else:
        print(f"Error: Source file '{args.source}' not found. Please run main.py or synthetic_data.py first.")
        sys.exit(1)

print(f"Loading POS transactions from {source_path}...")
df = pd.read_csv(source_path).sort_values("timestamp", kind="stable")
df = df.astype(object).where(df.notna(), None)          # NaN -> null in JSON

failed = 0


def on_error(exc):
    global failed
    failed += 1
    print("DELIVERY FAILED:", exc)


def json_serial(obj):
    if hasattr(obj, "item"):
        return obj.item()
    return str(obj)


producer = None
if not args.dry_run:
    print(f"Connecting to Apache Kafka at {args.bootstrap} (topic: {TOPIC})...")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v, default=json_serial).encode("utf-8"),
    )

sent = 0
for tx_id, g in df.groupby("transaction_id", sort=False):
    first = g.iloc[0]
    event = {
        "event_id": tx_id,                       # unique key the consumer uses to drop duplicates
        "transaction_id": tx_id,
        "store_id": first["store_id"],
        "timestamp": first["timestamp"],
        "lines": g[["line_number", "product_id", "quantity", "unit_price_kes"]].to_dict("records"),
    }
    if args.dry_run:
        print(json.dumps(event, default=json_serial))
    else:
        future = producer.send(TOPIC, key=event["store_id"], value=event)
        future.add_errback(on_error)

    sent += 1
    if sent % 1000 == 0:
        print(f"Sent {sent} receipts...")
    if args.limit and sent >= args.limit:
        break
    if args.delay:
        time.sleep(args.delay)

if producer:
    print("Flushing messages to Kafka...")
    producer.flush()

print(f"Done! Receipts sent: {sent}, failed: {failed}")
