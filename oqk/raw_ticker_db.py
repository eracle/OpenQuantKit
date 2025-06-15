import os
from datetime import date
from typing import List

import duckdb
import pandas as pd
from pandas.tseries.offsets import BDay

from .ticker_downloader import populate_tickers_from_exchange

DB_PATH = "tickers.duckdb"
RAW_TABLE = "raw_tickers"


def get_safe_lag_date() -> date:
    """Return the last business day."""
    return (pd.Timestamp.today() - BDay(1)).date()


def init_raw_ticker_table(db_path: str = DB_PATH) -> None:
    """Ensure the raw_tickers table exists."""
    con = duckdb.connect(db_path)
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            symbol TEXT PRIMARY KEY,
            is_bad BOOLEAN DEFAULT FALSE
        )
        """
    )
    con.close()


def ensure_raw_tickers_initialized(db_path: str = DB_PATH) -> None:
    """Create the table and populate tickers if missing."""
    db_exists = os.path.exists(db_path)
    init_raw_ticker_table(db_path)

    if not db_exists:
        print("🆕 Database file didn't exist. Initializing fresh ticker data...")
        populate_tickers_from_exchange(db_path)


def get_valid_tickers(db_path: str = DB_PATH) -> List[str]:
    """Return all tickers not marked as bad."""
    ensure_raw_tickers_initialized(db_path)
    con = duckdb.connect(db_path)
    rows = con.execute(f"SELECT symbol FROM {RAW_TABLE} WHERE is_bad = FALSE ORDER BY symbol").fetchall()
    con.close()
    return [r[0] for r in rows]


def mark_ticker_as_bad(symbol: str, db_path: str = DB_PATH) -> None:
    """Mark a ticker as bad in the raw_tickers table."""
    print(f"Bad ticker: {symbol}")
    con = duckdb.connect(db_path)
    con.execute(f"UPDATE {RAW_TABLE} SET is_bad = TRUE WHERE symbol = ?", (symbol,))
    con.close()


def print_raw_ticker_table_stats(db_path: str = DB_PATH) -> None:
    """Print statistics about the raw tickers table."""
    con = duckdb.connect(db_path)
    print("\n📊 Raw Ticker Table Summary:")
    try:
        total = con.execute(f"SELECT COUNT(*) FROM {RAW_TABLE}").fetchone()[0]
        bad = con.execute(f"SELECT COUNT(*) FROM {RAW_TABLE} WHERE is_bad = TRUE").fetchone()[0]

        print(f"  • Total tickers : {total}")
        print(f"  • Bad tickers   : {bad}")
    except Exception as e:
        print(f"  Error gathering analytics: {e}")

    con.close()
