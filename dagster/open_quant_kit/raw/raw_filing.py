# dagster/open_quant_kit/raw/raw_filing.py

import os
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from tqdm import tqdm

from dagster import asset, AssetDep, AssetExecutionContext

SEC_USER_AGENT = os.getenv("SEC_CONTACT_EMAIL", "your@email.com")


def ensure_raw_filing_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_filing (
                ticker TEXT NOT NULL,
                cik TEXT NOT NULL,
                accession TEXT NOT NULL,
                form_type TEXT,
                filing_date DATE,
                content TEXT,
                downloaded_at TIMESTAMP,
                PRIMARY KEY (ticker, accession)
            );
        """))


def download_filing_text(url: str) -> str | None:
    try:
        headers = {"User-Agent": SEC_USER_AGENT}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[ERROR] Failed to download {url}: {e}")
        return None


def insert_filings(engine, filings: list[dict]) -> int:
    if not filings:
        return 0
    try:
        df = pd.DataFrame(filings)
        df.to_sql("raw_filing", con=engine, if_exists="append", index=False, method="multi")
        return len(df)
    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
        return 0


def download_filings(engine, filings_df: pd.DataFrame, logger=print, batch_size: int = 25) -> int:
    inserted = 0
    rows = []

    for i, row in tqdm(filings_df.iterrows(), total=len(filings_df), desc="Downloading filings"):
        ticker = row["ticker"]
        cik = row["cik"]
        form = row["form_type"]
        date_filed = pd.to_datetime(row["date_filed"]).date()
        url = row["full_url"]

        accession = os.path.basename(url).replace(".txt", "")
        year = row.get("year", "unknown")
        quarter = row.get("quarter", "unknown")

        logger(f"{ticker}: Downloading {form} from {url} ({year} Q{quarter})")

        content = download_filing_text(url)
        if not content:
            continue

        rows.append({
            "ticker": ticker,
            "cik": cik,
            "accession": accession,
            "form_type": form,
            "filing_date": date_filed,
            "content": content,
            "downloaded_at": datetime.utcnow()
        })

        if len(rows) >= batch_size:
            inserted += insert_filings(engine, rows)
            rows = []

    # Insert any remaining rows
    if rows:
        inserted += insert_filings(engine, rows)

    logger(f"[INFO] Inserted {inserted} filings to raw_filing")
    return inserted


def run_raw_filing_ingestion(engine, logger=print) -> int:
    ensure_raw_filing_schema(engine)

    try:
        df = pd.read_sql("SELECT * FROM stg_filing_download_queue", con=engine)
    except Exception as e:
        logger(f"[ERROR] Could not read stg_filing_download_queue: {e}")
        return 0

    logger(f"[INFO] Found {len(df)} filings to download")
    return download_filings(engine, df, logger=logger)


@asset(
    compute_kind="python",
    required_resource_keys={"dbt_postgres"},
    deps=[AssetDep("stg_filing_download_queue")],
)
def raw_filing(context: AssetExecutionContext):
    engine = context.resources.dbt_postgres
    inserted = run_raw_filing_ingestion(engine, logger=context.log.info)
    context.log.info(f"raw_filing complete. Inserted: {inserted}")


# CLI entrypoint
if __name__ == "__main__":
    DATABASE_URL = os.getenv("POSTGRES_DB_URL", "").replace("postgres://", "postgresql://")
    if not DATABASE_URL:
        raise ValueError("POSTGRES_DB_URL is not set")

    engine = create_engine(DATABASE_URL)
    run_raw_filing_ingestion(engine)
