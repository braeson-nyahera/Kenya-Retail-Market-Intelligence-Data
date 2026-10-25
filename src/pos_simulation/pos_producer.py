"""Simple POS producer: reads historical_pos_lines.csv and sends one Kafka message per receipt.

  python pos_producer.py --dry-run --limit 3      # print only, no Kafka needed
  python pos_producer.py --delay 0.01             # send, ~100 receipts/second
"""
import argparse
import json
import os
import time

import pandas as pd
from kafka import Producer

TOPIC = "pos.transactions"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")   # inside Docker: kafka:9092

ap = argparse.ArgumentParser()
ap.add_argument("--source", default="data/synthetic/historical_pos_lines.csv")
ap.add_argument("--delay", type=float, default=0, help="seconds to wait between receipts")
ap.add_argument("--limit", type=int, help="stop after N receipts")
ap.add_argument("--dry-run", action="store_true")
args = ap.parse_args()

df = pd.read_csv(args.source).sort_values("timestamp", kind="stable")
df = df.astype(object).where(df.notna(), None)          # NaN -> null in JSON

producer = None if args.dry_run else Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,     # no duplicates from producer retries
    "acks": "all",
})
failed = 0


def on_delivery(err, msg):
    global failed
    if err:
        failed += 1
        print("DELIVERY FAILED:", err)


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
    value = json.dumps(event)
    if args.dry_run:
        print(value)
    else:
        producer.produce(TOPIC, key=str(event["store_id"]), value=value, on_delivery=on_delivery)
        producer.poll(0)
    sent += 1
    if args.limit and sent >= args.limit:
        break
    if args.delay:
        time.sleep(args.delay)

if producer:
    producer.flush()
print(f"receipts sent: {sent}, failed: {failed}")
