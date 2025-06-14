import os
from datetime import date
from typing import List

import duckdb
import pandas as pd
from pandas.tseries.offsets import BDay

TABLE_NAME = "prices"
DB_PATH = "tickers.duckdb"


def get_safe_lag_date() -> date:
    return (pd.Timestamp.today() - BDay(1)).date()


def ensure_tickers_initialized(db_path: str = DB_PATH) -> None:
    """Ensure the tickers DB file exists, the table is created, and populated if empty."""
    db_exists = os.path.exists(db_path)

    # Step 1: Create the DB and table if missing
    init_ticker_table(db_path)

    # Step 2: Populate only if the DB is new or table is empty
    if not db_exists:
        print("🆕 Database file didn't exist. Initializing fresh ticker data...")
    populate_tickers_from_exchange(db_path)


def get_all_valid_tickers(db_path: str = DB_PATH) -> List[str]:
    """Return tickers not marked as bad and needing update, sorted by oldest max_date first."""
    ensure_tickers_initialized()

    con = duckdb.connect(db_path)

    # Get the most recent market day
    safe_lag_date = get_safe_lag_date()

    rows = con.execute("""
        SELECT symbol
        FROM tickers
        WHERE is_bad = FALSE AND (max_date IS NULL OR max_date < ?)
        ORDER BY max_date NULLS FIRST
    """, (safe_lag_date,)).fetchall()

    con.close()
    return [row[0] for row in rows]


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


def print_ticker_table_stats(db_path: str = DB_PATH) -> None:
    """Print summary statistics and sample data from the tickers table."""
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
        if df.empty:
            print("  (empty)")
        else:
            print(df)
    except Exception as e:
        print(f"  Error fetching sample rows: {e}")

    con.close()


def populate_tickers_from_exchange(db_path: str = DB_PATH) -> None:
    """Download NASDAQ and NYSE tickers and insert them into the DB."""
    con = duckdb.connect(db_path)

    count = con.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
    if count > 0:
        con.close()
        return  # Already populated

    print("Downloading NASDAQ/NYSE tickers...")

    nasdaq = pd.read_csv(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
        sep="|"
    )
    nyse = pd.read_csv(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt",
        sep="|"
    )

    symbols = set(nasdaq["Symbol"].dropna().tolist() + nyse["ACT Symbol"].dropna().tolist())
    symbols = sorted(t for t in symbols if "test" not in t.lower())

    con.executemany(
        "INSERT INTO tickers (symbol) VALUES (?)",
        [(s,) for s in symbols]
    )
    con.close()

    print(f"Saved {len(symbols)} tickers to DuckDB")


def mark_ticker_as_bad(symbol: str, db_path: str = DB_PATH) -> None:
    print(f"Bad ticker:{symbol}")
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
