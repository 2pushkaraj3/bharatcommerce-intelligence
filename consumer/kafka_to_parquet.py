"""
Bharatcommerce Intelligence Platform
kafka_to_parquet.py — Kafka Consumer → Parquet Data Lake

Run: python kafka_to_parquet.py

Reads from: orders.raw, returns.raw
Writes to:  data_lake/
              orders/event_date=YYYY-MM-DD/state=X/batch_XXXXXX.parquet
              returns/event_date=YYYY-MM-DD/state=X/batch_XXXXXX.parquet

Why partition by date + state?
  - Mirrors the Kafka partition key (state) — no cross-partition reads
  - BigQuery and dbt can prune on load — only reads new data
  - Makes the local data_lake/ easy to inspect in VS Code

Env vars (optional):
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  DATA_LAKE_PATH            default: ./data_lake
  BATCH_SIZE                default: 30
  FLUSH_INTERVAL_SECS       default: 15
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bc.consumer")

DATA_LAKE      = Path(os.getenv("DATA_LAKE_PATH", "./data_lake"))
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "30"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL_SECS", "15"))

# ── Schemas ───────────────────────────────────────────────────

ORDER_SCHEMA = pa.schema([
    ("order_id",             pa.string()),
    ("event_timestamp",      pa.string()),
    ("customer_id",          pa.string()),
    ("customer_name",        pa.string()),
    ("customer_tier",        pa.int8()),
    ("city",                 pa.string()),
    ("state",                pa.string()),
    ("pincode",              pa.string()),
    ("category",             pa.string()),
    ("subcategory",          pa.string()),
    ("quantity",             pa.int16()),
    ("unit_price_inr",       pa.float32()),
    ("total_amount_inr",     pa.float32()),
    ("payment_method",       pa.string()),
    ("warehouse_id",         pa.string()),
    ("estimated_days",       pa.int8()),
    ("is_express",           pa.bool_()),
    ("expected_return_rate", pa.float32()),
    ("is_anomalous",         pa.bool_()),
    ("anomaly_reason",       pa.string()),
    ("ingestion_id",         pa.string()),
    ("consumed_at",          pa.string()),
])

RETURN_SCHEMA = pa.schema([
    ("return_id",         pa.string()),
    ("order_id",          pa.string()),
    ("event_timestamp",   pa.string()),
    ("customer_id",       pa.string()),
    ("city",              pa.string()),
    ("state",             pa.string()),
    ("category",          pa.string()),
    ("return_reason",     pa.string()),
    ("refund_amount_inr", pa.float32()),
    ("refund_method",     pa.string()),
    ("ingestion_id",      pa.string()),
    ("consumed_at",       pa.string()),
])

SCHEMAS = {"orders": ORDER_SCHEMA, "returns": RETURN_SCHEMA}


def write_batch(records: list[dict], topic_dir: str, schema: pa.Schema) -> int:
    """Write a list of records as partitioned Parquet. Returns row count written."""
    by_partition: dict[tuple, list] = defaultdict(list)
    now_str = datetime.now(tz=timezone.utc).isoformat()

    for r in records:
        r["consumed_at"] = now_str
        date  = r.get("event_timestamp", now_str)[:10]
        state = r.get("state", "Unknown").replace(" ", "_")
        by_partition[(date, state)].append(r)

    written = 0
    for (date, state), rows in by_partition.items():
        folder = DATA_LAKE / topic_dir / f"event_date={date}" / f"state={state}"
        folder.mkdir(parents=True, exist_ok=True)
        fname = folder / f"batch_{int(time.time()*1000) % 10_000_000:07d}.parquet"
        pq.write_table(
            pa.Table.from_pylist(rows, schema=schema),
            fname,
            compression="snappy",
        )
        written += len(rows)
    return written


def run():
    consumer = Consumer({
        "bootstrap.servers":    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id":             "bc-lake-writer-v1",
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   True,   # manual commit after successful write
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe(["orders.raw", "returns.raw"])

    order_buf:  list[dict] = []
    return_buf: list[dict] = []
    last_flush  = time.monotonic()
    total_rows  = 0
    total_errors= 0

    log.info("Consumer started | batch=%d | flush every %ds", BATCH_SIZE, FLUSH_INTERVAL)
    log.info("Writing to: %s", DATA_LAKE.resolve())
    log.info("Press Ctrl+C to stop")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.monotonic()

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                    total_errors += 1
            else:
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    if msg.topic() == "orders.raw":
                        order_buf.append(payload)
                    elif msg.topic() == "returns.raw":
                        return_buf.append(payload)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    log.warning("Bad message: %s", e)
                    total_errors += 1

            # Flush when batch is full or time elapsed
            should_flush = (
                len(order_buf)  >= BATCH_SIZE or
                len(return_buf) >= BATCH_SIZE or
                (now - last_flush) >= FLUSH_INTERVAL
            )

            if should_flush and (order_buf or return_buf):
                written = 0
                if order_buf:
                    written += write_batch(order_buf,  "orders",  ORDER_SCHEMA)
                    order_buf.clear()
                if return_buf:
                    written += write_batch(return_buf, "returns", RETURN_SCHEMA)
                    return_buf.clear()

                consumer.commit(asynchronous=False)
                total_rows += written
                last_flush  = now
                log.info("Flushed %3d rows to Parquet | total=%d | errors=%d",
                         written, total_rows, total_errors)

    except KeyboardInterrupt:
        log.info("Stopping consumer...")
    finally:
        # Flush remaining buffer
        if order_buf:
            write_batch(order_buf, "orders", ORDER_SCHEMA)
        if return_buf:
            write_batch(return_buf, "returns", RETURN_SCHEMA)
        consumer.close()
        log.info("Consumer stopped. Total rows written: %d", total_rows)


if __name__ == "__main__":
    run()