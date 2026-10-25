"""Simple Bronze writer: Kafka pos.transactions -> JSON-lines files, bad records -> DLQ topic.

  python bronze_writer.py                       # run until Ctrl+C
  python bronze_writer.py --exit-when-idle 15   # stop after 15 s with no new messages

Offsets are committed ONLY after the batch file is written and the DLQ is flushed, so a crash
can re-read some messages (at-least-once). Duplicates are removed later in Spark on event_id.
"""
import argparse
import json
import os
import signal
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kafka import Consumer, Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")   # inside Docker: kafka:9092
TOPIC, DLQ_TOPIC = "pos.transactions", "pos.transactions.dlq"
OUT_DIR = Path("data/bronze/pos")
BATCH_SIZE, FLUSH_SECONDS = 5000, 5


def check(raw):
    """Return (event, None) if the message is valid, else (None, reason)."""
    try:
        e = json.loads(raw)
    except (ValueError, TypeError):
        return None, "malformed_json"
    if not isinstance(e, dict):
        return None, "not_an_object"
    for k in ("event_id", "transaction_id", "store_id", "timestamp"):
        if not e.get(k):
            return None, f"missing_{k}"
    lines = e.get("lines")
    if not isinstance(lines, list) or not lines:
        return None, "missing_lines"
    seen = set()
    for l in lines:
        if not isinstance(l, dict) or not l.get("product_id"):
            return None, "null_product_id"
        q, p = l.get("quantity"), l.get("unit_price_kes")
        if not isinstance(q, int) or q <= 0:
            return None, "bad_quantity"
        if not isinstance(p, (int, float)) or p < 0:
            return None, "bad_price"
        if l.get("line_number") in seen:
            return None, "duplicate_line_number"
        seen.add(l.get("line_number"))
    return e, None


def write_batch(rows):
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    folder = OUT_DIR / f"ingest_date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    final = folder / f"batch_{time.time_ns()}.jsonl"
    tmp = final.with_suffix(".tmp")                 # write then rename = never a half-written file
    tmp.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    os.replace(tmp, final)


def run(consumer, dlq, exit_when_idle=0):
    stats = Counter()
    rows, pending = [], 0
    last_flush = last_msg = time.time()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    def flush():
        nonlocal rows, pending, last_flush
        if rows:
            write_batch(rows)                       # 1. data on disk
        dlq.flush()                                 # 2. DLQ messages delivered
        if pending:
            consumer.commit(asynchronous=False)     # 3. only now mark messages as done
        stats["batches"] += 1 if rows else 0
        rows, pending, last_flush = [], 0, time.time()

    while running:
        msg = consumer.poll(1.0)
        now = time.time()
        if msg is None:
            if exit_when_idle and now - last_msg > exit_when_idle:
                break
        elif msg.error():
            print("consumer error:", msg.error())
        else:
            last_msg, pending = now, pending + 1
            event, reason = check(msg.value())
            if event:
                event.update(_partition=msg.partition(), _offset=msg.offset(),
                             _ingested_at=datetime.now(timezone.utc).isoformat())
                rows.append(event)
                stats["valid"] += 1
            else:
                dlq.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(),
                            headers=[("error", reason.encode()),
                                     ("source", f"{msg.topic()}[{msg.partition()}]@{msg.offset()}".encode())])
                dlq.poll(0)
                stats[f"dlq:{reason}"] += 1
        if pending and (pending >= BATCH_SIZE or now - last_flush >= FLUSH_SECONDS):
            flush()
    flush()
    consumer.close()
    print("summary:", dict(stats))
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--exit-when-idle", type=int, default=0, help="stop after N idle seconds")
    args = ap.parse_args()
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "bronze-writer",
        "enable.auto.commit": False,                # we commit manually, after writing
        "auto.offset.reset": "earliest",
    })
    c.subscribe([TOPIC])
    run(c, Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"}), args.exit_when_idle)
