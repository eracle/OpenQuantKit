# ticker_db.py

import os
from datetime import date
from typing import List

import duckdb
import pandas as pd
from pandas.tseries.offsets import BDay

from .ticker_downloader import populate_tickers_from_exchange

TABLE_NAME = "prices"
DB_PATH = "tickers.duckdb"


def get_safe_lag_date() -> date:
    return (pd.Timestamp.today() - BDay(1)).date()


def init_ticker_table(db_path: str = DB_PATH) -> None:
    """Ensure the tickers table exists."""
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS tickers (
            symbol TEXT PRIMARY KEY,
            max_date DATE,
            is_bad BOOLEAN DEFAULT FALSE
        )
    """)
    con.close()


def ensure_tickers_initialized(db_path: str = DB_PATH) -> None:
    """Ensure the tickers DB file exists, the table is created, and populated if empty."""
    db_exists = os.path.exists(db_path)
    init_ticker_table(db_path)

    if not db_exists:
        print("🆕 Database file didn't exist. Initializing fresh ticker data...")

    populate_tickers_from_exchange(db_path)


def get_all_valid_tickers(db_path: str = DB_PATH) -> List[str]:
    """Return tickers not marked as bad and needing update, sorted by oldest max_date first."""
    ensure_tickers_initialized(db_path)
    con = duckdb.connect(db_path)
    safe_lag_date = get_safe_lag_date()
    rows = con.execute("""
        SELECT symbol
        FROM tickers
        WHERE is_bad = FALSE AND (max_date IS NULL OR max_date < ?)
        ORDER BY max_date NULLS FIRST
    """, (safe_lag_date,)).fetchall()
    con.close()
    return [row[0] for row in rows]


def print_ticker_table_stats(db_path: str = DB_PATH) -> None:
    con = duckdb.connect(db_path)

    print("\n📊 Ticker Table Summary:")
    try:
        total = con.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
        bad = con.execute("SELECT COUNT(*) FROM tickers WHERE is_bad = TRUE").fetchone()[0]
        with_data = con.execute("SELECT COUNT(*) FROM tickers WHERE max_date IS NOT NULL").fetchone()[0]
        latest = con.execute("SELECT MAX(max_date) FROM tickers").fetchone()[0]
        earliest = con.execute("SELECT MIN(max_date) FROM tickers WHERE max_date IS NOT NULL").fetchone()[0]

        print(f"  • Total tickers       : {total}")
        print(f"  • Bad tickers         : {bad}")
        print(f"  • With price data     : {with_data}")
        print(f"  • Earliest max_date   : {earliest}")
        print(f"  • Latest max_date     : {latest}")
    except Exception as e:
        print(f"  Error gathering analytics: {e}")

    print("\n🧾 Sample rows:")
    try:
        df = con.execute("SELECT * FROM tickers ORDER BY symbol LIMIT 10").fetchdf()
        print("  (empty)" if df.empty else df)
    except Exception as e:
        print(f"  Error fetching sample rows: {e}")

    con.close()


def mark_ticker_as_bad(symbol: str, db_path: str = DB_PATH) -> None:
    print(f"Bad ticker: {symbol}")
    con = duckdb.connect(db_path)
    con.execute("UPDATE tickers SET is_bad = TRUE WHERE symbol = ?", (symbol,))
    con.close()


def update_max_date(symbol: str, max_date: str, db_path: str = DB_PATH) -> None:
    con = duckdb.connect(db_path)
    con.execute("""
        UPDATE tickers
        SET max_date = ?
        WHERE symbol = ?
    """, (max_date, symbol))
    con.close()


def get_ticker_max_date(ticker: str, data_dir: str) -> tuple[duckdb.DuckDBPyConnection, date | None]:
    """Open ticker DB file, return connection and max date from prices table."""
    db_path = os.path.join(data_dir, f"{ticker}.duckdb")
    conn = duckdb.connect(db_path)

    try:
        result = conn.execute(f"SELECT MAX(date) FROM {TABLE_NAME}").fetchone()
        max_date = result[0] if result and result[0] else None
    except Exception:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (date DATE, close DOUBLE)")
        max_date = None

    return conn, max_date
