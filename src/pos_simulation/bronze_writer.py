"""Simple Bronze writer using kafka-python for Apache Kafka:
Kafka pos.transactions -> JSON-lines files, bad records -> DLQ topic.

Usage:
  python src/pos_simulation/bronze_writer.py                       # run until Ctrl+C
  python src/pos_simulation/bronze_writer.py --exit-when-idle 15   # stop after 15 s with no new messages
"""
import argparse
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

Consumer = KafkaConsumer
Producer = KafkaProducer

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BOOTSTRAP") or "localhost:9092"
TOPIC, DLQ_TOPIC = "pos.transactions", "pos.transactions.dlq"
DEFAULT_OUT_DIR = Path("data/bronze/pos")
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


def resolve_out_dir(out_dir_path):
    p = Path(out_dir_path)
    if not p.is_absolute():
        repo_root = Path(__file__).resolve().parent.parent.parent
        if Path("data").exists():
            return p
        return repo_root / p
    return p


def write_batch(rows, out_dir):
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    folder = out_dir / f"ingest_date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    final = folder / f"batch_{time.time_ns()}.jsonl"
    tmp = final.with_suffix(".tmp")
    tmp.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    os.replace(tmp, final)
    print(f"Wrote batch of {len(rows)} records to {final}")


def run(consumer, dlq_producer, out_dir, exit_when_idle=0):
    stats = Counter()
    rows, pending = [], 0
    last_flush = last_msg = time.time()
    running = True

    def stop(*_):
        nonlocal running
        print("\nStopping consumer gracefully...")
        running = False

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)

    def flush():
        nonlocal rows, pending, last_flush
        if rows:
            write_batch(rows, out_dir)              # 1. data on disk
        dlq_producer.flush()                        # 2. DLQ messages delivered
        if pending:
            consumer.commit()                       # 3. commit offsets
        stats["batches"] += 1 if rows else 0
        rows, pending, last_flush = [], 0, time.time()

    print(f"Bronze writer started. Listening on '{TOPIC}' -> Output: {out_dir}")
    print("Press Ctrl+C to stop.")

    while running:
        records_dict = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        now = time.time()

        if not records_dict:
            if exit_when_idle and (now - last_msg > exit_when_idle):
                print(f"No new messages for {exit_when_idle} seconds. Exiting...")
                break
            if pending and (now - last_flush >= FLUSH_SECONDS):
                flush()
            continue

        for topic_partition, batch in records_dict.items():
            for msg in batch:
                last_msg = now
                pending += 1
                event, reason = check(msg.value)
                if event:
                    event.update(
                        _partition=msg.partition,
                        _offset=msg.offset,
                        _ingested_at=datetime.now(timezone.utc).isoformat()
                    )
                    rows.append(event)
                    stats["valid"] += 1
                else:
                    headers = [
                        ("error", reason.encode("utf-8")),
                        ("source", f"{msg.topic}[{msg.partition}]@{msg.offset}".encode("utf-8"))
                    ]
                    dlq_producer.send(
                        DLQ_TOPIC,
                        key=msg.key,
                        value=msg.value.encode("utf-8") if isinstance(msg.value, str) else msg.value,
                        headers=headers
                    )
                    stats[f"dlq:{reason}"] += 1

                if pending >= BATCH_SIZE or (now - last_flush >= FLUSH_SECONDS):
                    flush()

        if pending and (now - last_flush >= FLUSH_SECONDS):
            flush()

    flush()
    consumer.close()
    print("Run summary:", dict(stats))
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Bronze POS transactions Apache Kafka consumer & DLQ router")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server (default: %(default)s)")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory for bronze JSONL files")
    ap.add_argument("--exit-when-idle", type=int, default=0, help="stop after N idle seconds")
    args = ap.parse_args()

    target_out_dir = resolve_out_dir(args.out_dir)
    print(f"Connecting to Apache Kafka at {args.bootstrap}...")

    c = KafkaConsumer(
        TOPIC,
        bootstrap_servers=args.bootstrap,
        group_id="bronze-writer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: m.decode("utf-8", errors="replace"),
    )

    dlq_p = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        key_serializer=lambda k: k if isinstance(k, bytes) else (str(k).encode("utf-8") if k is not None else None),
        value_serializer=lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
    )

    run(c, dlq_p, target_out_dir, args.exit_when_idle)
