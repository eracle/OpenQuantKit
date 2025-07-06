# dagster/open_quant_kit/raw/raw_filing_index.py

import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from datetime import datetime
from tqdm import tqdm

from dagster import asset, AssetExecutionContext


def get_latest_ingested_quarter(engine):
    query = text("""
        SELECT year, quarter
        FROM raw_filing_index
        GROUP BY year, quarter
        ORDER BY year DESC, quarter DESC
        LIMIT 1
    """)
    try:
        df = pd.read_sql(query, con=engine)
        if df.empty:
            return None, None
        return int(df["year"].iloc[0]), int(df["quarter"].iloc[0])
    except Exception as e:
        print(f"[ERROR] Failed to retrieve latest year/quarter: {e}")
        return None, None


def ensure_raw_filing_index_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_filing_index (
                cik TEXT NOT NULL,
                company_name TEXT,
                form_type TEXT,
                date_filed DATE,
                filename TEXT,
                year INT,
                quarter INT,
                PRIMARY KEY (cik, form_type, date_filed, filename)
            );
        """))


def download_master_idx(year: int, quarter: int):
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    try:
        response = requests.get(url, headers={"User-Agent": "OpenQuantKit/1.0 <your@email.com>"})
        if response.status_code != 200:
            print(f"[WARN] Failed to download {url} (status {response.status_code})")
            return []

        lines = response.text.splitlines()
        start_index = next(i for i, line in enumerate(lines) if line.startswith("CIK|"))
        rows = []

        for line in lines[start_index + 1:]:
            parts = line.split("|")
            if len(parts) == 5:
                cik, name, form_type, date_filed, filename = parts
                try:
                    date_parsed = pd.to_datetime(date_filed, errors="coerce")
                    if pd.isna(date_parsed):
                        continue
                    filing_year = date_parsed.year
                    filing_quarter = (date_parsed.month - 1) // 3 + 1
                    rows.append({
                        "cik": str(cik).zfill(10),
                        "company_name": name,
                        "form_type": form_type,
                        "date_filed": date_parsed.date(),
                        "filename": filename,
                        "year": filing_year,
                        "quarter": filing_quarter,
                    })
                except Exception:
                    continue
        return rows
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return []


def insert_filing_index(engine, df: pd.DataFrame):
    if df.empty:
        return 0
    try:
        with engine.begin() as conn:
            df.to_sql("raw_filing_index", con=conn, if_exists="append", index=False, method="multi")
        return len(df)
    except Exception as e:
        print(f"[ERROR] Insert failed: {e}")
        return 0


def run_raw_filing_index_ingestion(engine, from_year=1994, logger=print):
    ensure_raw_filing_index_schema(engine)
    latest_year, latest_quarter = get_latest_ingested_quarter(engine)

    if latest_year and latest_quarter:
        logger(f"Latest existing filings: {latest_year} Q{latest_quarter}")
    else:
        logger("No existing filings found in database — starting from scratch.")

    today = datetime.today()
    total_inserted = 0

    for year in tqdm(range(from_year, today.year + 1), desc="Year"):
        for quarter in range(1, 5):
            # Skip future quarters
            if year == today.year and quarter > (today.month - 1) // 3 + 1:
                break

            # Skip previously ingested quarters
            if latest_year is not None:
                if year < latest_year or (year == latest_year and quarter <= latest_quarter):
                    continue

            filings = download_master_idx(year, quarter)
            if not filings:
                continue

            df = pd.DataFrame(filings)
            count = insert_filing_index(engine, df)
            logger(f"{year} Q{quarter}: Inserted {count}")
            total_inserted += count

    logger(f"[DONE] Total inserted: {total_inserted}")
    return total_inserted


@asset(
    compute_kind="python",
    required_resource_keys={"dbt_postgres"},
)
def raw_filing_index(context: AssetExecutionContext) -> None:
    engine = context.resources.dbt_postgres
    inserted = run_raw_filing_index_ingestion(engine, logger=context.log.info)
    context.log.info(f"raw_filing_index asset completed — inserted {inserted} rows.")


# CLI test hook
if __name__ == "__main__":
    DATABASE_URL = os.getenv('POSTGRES_DB_URL', '').replace("postgres://", "postgresql://")
    if not DATABASE_URL:
        raise ValueError("POSTGRES_DB_URL not set")

    engine = create_engine(DATABASE_URL)
    run_raw_filing_index_ingestion(engine)
