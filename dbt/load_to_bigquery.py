"""
load_to_bigquery.py
Uploads local Parquet files from data_lake/ into BigQuery raw tables.

Run once to seed BigQuery, then run on schedule (or via Airflow later).

Usage:
    python load_to_bigquery.py

Env:
    GOOGLE_APPLICATION_CREDENTIALS  path to gcp_key.json
    GCP_PROJECT                      your project id
    BQ_DATASET                       target dataset (default: bharatcommerce)
"""

import os
from pathlib import Path

from google.cloud import bigquery

PROJECT  = os.getenv("GCP_PROJECT", "sigma-composite-492018-p3")
DATASET  = os.getenv("BQ_DATASET",  "bharatcommerce")
KEY_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./gcp_key.json")
DATA_LAKE = Path("./data_lake")


def get_client():
    return bigquery.Client.from_service_account_json(KEY_FILE, project=PROJECT)


def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET} already exists")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"Created dataset {DATASET}")


def load_table(client, topic: str, bq_table: str):
    folder = DATA_LAKE / topic
    if not folder.exists():
        print(f"No data found at {folder} — skipping")
        return

    files = list(folder.rglob("*.parquet"))
    if not files:
        print(f"No Parquet files in {folder} — skipping")
        return

    table_ref = f"{PROJECT}.{DATASET}.{bq_table}"
    job_config = bigquery.LoadJobConfig(
        source_format        = bigquery.SourceFormat.PARQUET,
        write_disposition    = bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect           = True,
    )

    print(f"Loading {len(files)} files → {table_ref} ...")
    for i, f in enumerate(files, 1):
        with open(f, "rb") as fh:
            job = client.load_table_from_file(fh, table_ref, job_config=job_config)
            job.result()
        if i % 10 == 0:
            print(f"  {i}/{len(files)} files loaded")
        # After first file use WRITE_APPEND so we don't overwrite
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    table = client.get_table(table_ref)
    print(f"Done. {table.num_rows:,} rows in {table_ref}")


def run():
    print("=== Bharatcommerce BigQuery Loader ===")
    print(f"Project:  {PROJECT}")
    print(f"Dataset:  {DATASET}")
    print(f"Key file: {KEY_FILE}\n")

    client = get_client()
    ensure_dataset(client)

    load_table(client, "orders",  "orders_raw")
    load_table(client, "returns", "returns_raw")

    print("\nAll done. Now run: dbt run")


if __name__ == "__main__":
    run()