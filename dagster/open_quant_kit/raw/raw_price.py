import os
from datetime import timedelta

import pandas as pd
import yfinance as yf
from pandas.tseries.offsets import BDay
from sqlalchemy import create_engine, text
from tqdm import tqdm

from dagster import asset, AssetDep, AssetExecutionContext


def ensure_raw_price_schema(engine) -> None:
    """Ensure raw_price table and indexes exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_price (
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume BIGINT,
                PRIMARY KEY (ticker, date)
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_price_date ON raw_price(date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_price_ticker ON raw_price(ticker);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_price_ticker_date ON raw_price(ticker, date);"))


def get_safe_lag_date() -> pd.Timestamp:
    """Return last business day (normalized)."""
    return (pd.Timestamp.today() - BDay(1)).normalize()


def get_ticker_max_date_pg(engine, ticker: str) -> pd.Timestamp | None:
    """Get the latest date for which we have data for this ticker."""
    query = text("""
        SELECT MAX(date) AS max_date
        FROM raw_price
        WHERE ticker = :ticker
    """)
    try:
        df = pd.read_sql(query, con=engine, params={"ticker": ticker})
        max_date = df["max_date"].iloc[0]
        return pd.to_datetime(max_date) if pd.notnull(max_date) else None
    except Exception as e:
        print(f"{ticker}: Error querying max date - {e}")
        return None


def upsert_price_data_pg(engine, df: pd.DataFrame) -> int:
    """Insert price data using Pandas with primary key constraint to avoid duplicates."""
    if df.empty:
        return 0

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    try:
        df.to_sql("raw_price", con=engine, if_exists="append", index=False, method="multi")
        return len(df)
    except Exception as e:
        print(f"Error writing to raw_price: {e}")
        return 0


def update_ticker_pg(engine, ticker: str) -> tuple[pd.Timestamp | None, int]:
    """Download and update price data for one ticker with full history support."""
    end_date = get_safe_lag_date()
    start_date = None

    max_date = get_ticker_max_date_pg(engine, ticker)
    if max_date and max_date >= end_date:
        print(f"{ticker}: Up to date ({max_date.date()})")
        return max_date, 0
    elif max_date:
        start_date = max_date + timedelta(days=1)
        if start_date > end_date:
            print(f"{ticker}: Nothing to fetch")
            return max_date, 0

    # Build download parameters
    download_kwargs = {
        "tickers": ticker,
        "end": end_date + timedelta(days=1),
        "progress": False,
        "auto_adjust": True,
        "threads": False,
        "multi_level_index": False,
    }

    if start_date is None:
        download_kwargs["period"] = "max"
    else:
        download_kwargs["start"] = start_date

    try:
        df = yf.download(**download_kwargs)
    except Exception as e:
        print(f"{ticker}: Download error - {e}")
        return None, 0

    if df.empty:
        print(f"{ticker}: No data")
        return None, 0

    df = df.reset_index()
    df = df.rename(columns={col: col.lower() for col in df.columns})

    required_cols = {"open", "high", "low", "close", "volume", "date"}
    available_cols = set(df.columns)
    missing_required = required_cols - available_cols
    if missing_required:
        print(
            f"{ticker}: Missing required columns {missing_required}, skipping. Available columns: {sorted(available_cols)}")
        return None, 0

    df["ticker"] = ticker
    df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]

    row_count = upsert_price_data_pg(engine, df)

    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    print(f"{ticker}: Inserted {row_count} rows — Range: {min_date} → {max_date}")

    return df["date"].max(), row_count


def run_raw_price_ingestion(engine, logger=print) -> int:
    """Core logic to ingest price data for all tickers."""
    ensure_raw_price_schema(engine)

    logger("[INFO] Reading tickers from dim_ticker")
    try:
        tickers_df = pd.read_sql("SELECT symbol FROM dim_ticker", con=engine)
        tickers = tickers_df["symbol"].dropna().unique().tolist()
    except Exception as e:
        logger(f"[ERROR] Failed to read dim_ticker: {e}")
        raise

    logger(f"[INFO] Updating {len(tickers)} tickers")

    total_inserted = 0
    skipped_tickers = []

    for ticker in tqdm(tickers, desc="Updating prices"):
        _, inserted = update_ticker_pg(engine, ticker)
        total_inserted += inserted
        if inserted == 0:
            skipped_tickers.append(ticker)

    logger(f"[INFO] Ingestion completed. Total rows inserted: {total_inserted}")
    logger(f"[INFO] Skipped tickers: {len(skipped_tickers)}")

    if skipped_tickers:
        logger(f"[INFO] First 10 skipped tickers: {skipped_tickers[:10]}")

    return total_inserted


@asset(
    compute_kind="python",
    required_resource_keys={"dbt_postgres"},
    deps=[AssetDep("dim_ticker")],
)
def raw_price(context: AssetExecutionContext) -> None:
    """Dagster asset that wraps raw price ingestion."""
    engine = context.resources.dbt_postgres
    inserted = run_raw_price_ingestion(engine, logger=context.log.info)
    context.log.info(f"raw_price asset completed successfully. Inserted: {inserted}")


# CLI entry point
if __name__ == "__main__":
    DATABASE_URL = os.getenv('POSTGRES_DB_URL', '').replace("postgres://", "postgresql://")
    if not DATABASE_URL:
        raise ValueError("POSTGRES_DB_URL environment variable not set")

    engine = create_engine(DATABASE_URL)
    run_raw_price_ingestion(engine)
