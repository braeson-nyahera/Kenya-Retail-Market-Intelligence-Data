# Kenya Retail Market Intelligence Data Platform

Retail market data intelligence and transaction streaming platform for Kenyan market commodities, CPI indicators, and POS retail transactions.

---

## Architecture Overview

```
                      +-----------------------------+
                      |   historical_pos_lines.csv  |
                      +--------------+--------------+
                                     |
                                     v
                       [ pos_producer.py ]
                                     |
                                     v
                    Apache Kafka: pos.transactions
                                     |
                     +---------------+---------------+
                     |                               |
          (Valid transactions)              (Anomalies / Dirt)
                     |                               |
                     v                               v
           [ bronze_writer.py ]           Kafka: pos.transactions.dlq
                     |
                     v
          data/bronze/pos/
          ingest_date=YYYY-MM-DD/
          batch_<timestamp>.jsonl
```

---

## 1. Starting Kafka & Kafka UI

Kafka and Kafka UI are grouped under the `kafka` profile:

```bash
docker compose --profile kafka up -d
```

- **Kafka UI**: Open your browser at [http://localhost:8080](http://localhost:8080)
- **Broker**: `localhost:9092` (host) / `kafka:9092` (Docker network)

---

## 2. Running the POS Simulation

### Option A: Using Docker Compose Profile (Recommended)
You can trigger the entire simulation as a containerized service with one command:

```bash
# Trigger the simulation (starts Bronze consumer + streams 1,000 receipts):
docker compose --profile simulation up
```

You can also run the producer or consumer independently via Docker Compose:

```bash
# Run only the producer:
docker compose --profile producer up

# Run only the bronze consumer:
docker compose --profile consumer up
```

All ingested batches are written directly to `./data/bronze/pos/` on your host machine.

---

### Option B: Running from Host Terminal (Python)

If you prefer running directly from your host environment:

```bash
# Install dependencies
uv sync # or: pip install kafka-python pandas

# Run all-in-one:
python src/pos_simulation/run_all.py --limit 1000 --delay 0.01

# Or run consumer and producer in separate terminals:
# Terminal 1:
python src/pos_simulation/bronze_writer.py --exit-when-idle 15
# Terminal 2:
python src/pos_simulation/pos_producer.py --limit 1000 --delay 0.01
```

---

## 3. Data Ingestion & Quality Checks

The Bronze writer validates each receipt:
- **Valid receipts**: Stored as partitioned JSON-Lines in `data/bronze/pos/ingest_date=YYYY-MM-DD/batch_<timestamp>.jsonl` with partition/offset metadata.
- **Injected anomalies**: Transactions with negative quantities, bad prices, or missing products are redirected to the Dead Letter Queue (`pos.transactions.dlq`) with header annotations describing the failure reason.
