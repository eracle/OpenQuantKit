# # oqk/ticker_db.py

import os
from datetime import date

import duckdb
import pandas as pd
from pandas.tseries.offsets import BDay

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
            is_bad BOOLEAN DEFAULT FALSE,
            data_duration_days INTEGER,
            num_data_points INTEGER,
            completeness_ratio DOUBLE,
            largest_gap_days INTEGER,
            num_gaps_gt_3_days INTEGER,
            num_gaps_gt_5_days INTEGER,
            std_close DOUBLE,
            num_zero_close INTEGER,
            first_date DATE,
            last_date DATE,
            weekday_coverage DOUBLE,
            has_recent_data BOOLEAN,
            num_duplicate_dates INTEGER
        )
    """)
    con.close()


def update_ticker_metrics(symbol: str, metrics: dict, db_path: str = DB_PATH) -> None:
    con = duckdb.connect(db_path)
    keys = ", ".join(metrics.keys())
    placeholders = ", ".join(["?"] * len(metrics))
    values = list(metrics.values())
    sql = f"""
        UPDATE tickers
        SET ({keys}) = ({placeholders})
        WHERE symbol = ?
    """
    con.execute(sql, (*values, symbol))
    con.close()


def ensure_tickers_initialized(db_path: str = DB_PATH) -> None:
    """Ensure the tickers DB file exists, the table is created, and populated if empty."""
    db_exists = os.path.exists(db_path)
    init_ticker_table(db_path)

    if not db_exists:
        print("🆕 Database file didn't exist. Initializing fresh ticker data...")


