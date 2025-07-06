import json
import os

import pandas as pd
from sqlalchemy import create_engine, text

from dagster import asset, AssetExecutionContext


def run_dim_cik_ingestion(engine, logger=print) -> int:
    json_path = "/app/data/company_tickers.json"
    if not os.path.exists(json_path):
        logger(f"[ERROR] JSON file not found: {json_path}")
        return 0

    try:
        with open(json_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger(f"[ERROR] Failed to load JSON: {e}")
        return 0

    records = []
    for _, entry in data.items():
        records.append({
            "ticker": entry["ticker"],
            "cik": str(entry["cik_str"]).zfill(10),
            "title": entry["title"]
        })

    df = pd.DataFrame(records)
    logger(f"[INFO] Parsed {len(df)} rows from {json_path}")

    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS dim_cik"))
            df.to_sql("dim_cik", con=conn, index=False)
        logger("[INFO] dim_cik table written to database")
        return len(df)
    except Exception as e:
        logger(f"[ERROR] Failed to write dim_cik table: {e}")
        return 0


@asset(
    compute_kind="python",
    required_resource_keys={"dbt_postgres"},
)
def dim_cik(context: AssetExecutionContext) -> None:
    engine = context.resources.dbt_postgres
    count = run_dim_cik_ingestion(engine, logger=context.log.info)
    context.log.info(f"dim_cik asset completed — inserted {count} rows.")


# CLI entrypoint
if __name__ == "__main__":
    DATABASE_URL = os.getenv("POSTGRES_DB_URL", "").replace("postgres://", "postgresql://")
    if not DATABASE_URL:
        raise ValueError("POSTGRES_DB_URL is not set")

    engine = create_engine(DATABASE_URL)
    count = run_dim_cik_ingestion(engine)
    print(f"[DONE] Loaded {count} rows into dim_cik")
