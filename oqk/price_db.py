import os
from datetime import date
import duckdb

TABLE_NAME = "prices"


def get_price_db_path(ticker: str, data_dir: str = "data") -> str:
    return os.path.join(data_dir, f"{ticker}.duckdb")


def get_ticker_max_date(ticker: str, data_dir: str = "data") -> tuple[duckdb.DuckDBPyConnection, date | None]:
    """
    Opens or creates the per-ticker price DB and ensures the table exists.
    Returns the DB connection and max recorded price date (or None).
    """
    db_path = get_price_db_path(ticker, data_dir)
    conn = duckdb.connect(db_path)

    try:
        result = conn.execute(f"SELECT MAX(date) FROM {TABLE_NAME}").fetchone()
        max_date = result[0] if result and result[0] else None
    except Exception:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (date DATE, close DOUBLE)")
        max_date = None

    return conn, max_date
