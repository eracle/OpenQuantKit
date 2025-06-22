import os
from datetime import timedelta

import pandas as pd
import yfinance as yf
from pandas.tseries.offsets import BDay
from sqlalchemy import create_engine
from sqlalchemy import text
from tqdm import tqdm

from dagster import asset, AssetDep, AssetExecutionContext


def ensure_raw_price_schema(engine) -> None:
    """Ensure raw_price table and indexes exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_price (
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (ticker, date)
            );
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_raw_price_date ON raw_price(date);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_raw_price_ticker ON raw_price(ticker);
        """))


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


def upsert_price_data_pg(engine, df: pd.DataFrame) -> None:
    """Insert price data using Pandas (assumes table has PK)."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    try:
        df.to_sql("raw_price", con=engine, if_exists="append", index=False, method="multi")
    except Exception as e:
        print("Error writing to raw_price:", e)
        raise


def update_ticker_pg(engine, ticker: str) -> pd.Timestamp | None:
    """Download and update price data for one ticker."""
    end_date = get_safe_lag_date()
    start_date = None

    max_date = get_ticker_max_date_pg(engine, ticker)
    if max_date and max_date >= end_date:
        print(f"{ticker}: Up to date ({max_date.date()})")
        return max_date
    elif max_date:
        start_date = max_date + timedelta(days=1)
        if start_date > end_date:
            print(f"{ticker}: Nothing to fetch")
            return max_date

    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date + timedelta(days=1),
            progress=False,
            auto_adjust=True,
            threads=False,
            multi_level_index=False,
        )
    except Exception as e:
        print(f"{ticker}: Download error - {e}")
        return None

    if df.empty:
        print(f"{ticker}: No data")
        return None

    df = df.reset_index()
    df = df.rename(columns={col: col.lower() for col in df.columns})
    if "date" not in df.columns or "close" not in df.columns:
        print(f"{ticker}: Missing required columns")
        return None

    df["ticker"] = ticker
    df = df[["ticker", "date", "close"]]
    upsert_price_data_pg(engine, df)

    print(f"{ticker}: Updated to {df['date'].max().date()}")
    return df["date"].max()


@asset(
    compute_kind="python",
    required_resource_keys={"dbt_postgres"},
    deps=[AssetDep("dim_ticker")],
)
def raw_price(context: AssetExecutionContext) -> None:
    """
    Dagster asset that ingests price data from Yahoo Finance
    into the raw_price Postgres table.
    """
    engine = context.resources.dbt_postgres

    context.log.info("Ensuring raw_price table and indexes exist")
    ensure_raw_price_schema(engine)

    context.log.info("Reading tickers from dim_ticker")
    try:
        tickers_df = pd.read_sql("SELECT symbol FROM dim_ticker", con=engine)
        tickers = tickers_df["symbol"].dropna().unique().tolist()
    except Exception as e:
        context.log.error(f"Failed to read dim_ticker: {e}")
        raise

    context.log.info(f"Updating {len(tickers)} tickers")
    for ticker in tqdm(tickers, desc="Updating prices"):
        update_ticker_pg(engine, ticker)

    context.log.info("raw_price asset completed successfully")





if __name__ == "__main__":
    class DummyLog:
        def info(self, msg): print("[INFO]", msg)

        def error(self, msg): print("[ERROR]", msg)


    class DummyContext:
        def __init__(self, engine):
            self.resources = {'dbt_postgres': engine}
            self.log = DummyLog()


    # Set this to your local Postgres connection string or use .env
    DATABASE_URL = os.getenv('POSTGRES_DB_URL').replace("postgres://", "postgresql://")

    engine = create_engine(DATABASE_URL)

    context = DummyContext(engine)

    # Call the Dagster asset directly
    raw_price(context)
