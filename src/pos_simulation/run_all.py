"""End-to-End POS Simulation Runner:
Launches the Bronze writer consumer and POS transaction producer together.

Usage:
  python src/pos_simulation/run_all.py --limit 500               # produce 500 receipts and consume them
  python src/pos_simulation/run_all.py --delay 0.01 --limit 1000 # throttle production to ~100/sec
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BOOTSTRAP") or "localhost:9092"


def main():
    ap = argparse.ArgumentParser(description="Run Kafka POS Simulation (Producer + Bronze Consumer)")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server (default: %(default)s)")
    ap.add_argument("--limit", type=int, default=500, help="Number of receipts to produce (default: 500)")
    ap.add_argument("--delay", type=float, default=0, help="Delay in seconds between receipts (default: 0)")
    ap.add_argument("--exit-when-idle", type=int, default=8, help="Idle seconds before consumer exits (default: 8)")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parent.parent.parent
    producer_script = repo_root / "src" / "pos_simulation" / "pos_producer.py"
    consumer_script = repo_root / "src" / "pos_simulation" / "bronze_writer.py"

    print("=" * 60)
    print("Starting Kenya Retail Market POS Simulation")
    print(f"Bootstrap Server: {args.bootstrap}")
    print(f"Target Receipts:  {args.limit}")
    print(f"Receipt Delay:    {args.delay}s")
    print("=" * 60)

    # 1. Start the Bronze writer consumer in the background
    consumer_cmd = [
        sys.executable,
        str(consumer_script),
        "--bootstrap", args.bootstrap,
        "--exit-when-idle", str(args.exit_when_idle),
    ]
    print("\n[1/2] Starting Bronze Writer Consumer...")
    consumer_proc = subprocess.Popen(consumer_cmd, cwd=str(repo_root))

    # Give consumer a moment to connect and subscribe
    time.sleep(2)

    # 2. Run the POS producer synchronously
    producer_cmd = [
        sys.executable,
        str(producer_script),
        "--bootstrap", args.bootstrap,
        "--limit", str(args.limit),
        "--delay", str(args.delay),
    ]
    print("\n[2/2] Running POS Producer...")
    producer_proc = subprocess.run(producer_cmd, cwd=str(repo_root))

    if producer_proc.returncode != 0:
        print(f"Warning: Producer exited with code {producer_proc.returncode}")

    print("\nWaiting for Bronze Writer to finish processing remaining batches...")
    consumer_proc.wait()

    print("\n" + "=" * 60)
    print("Simulation Complete!")
    print(f"Check output directory: {repo_root / 'data' / 'bronze' / 'pos'}")
    print("=" * 60)


if __name__ == "__main__":
    main()

