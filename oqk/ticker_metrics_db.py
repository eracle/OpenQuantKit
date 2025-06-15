from datetime import date
from typing import List

import duckdb

from .raw_ticker_db import RAW_TABLE, ensure_raw_tickers_initialized, get_safe_lag_date

DB_PATH = "tickers.duckdb"
METRICS_TABLE = "ticker_stats"


def init_metrics_table(db_path: str = DB_PATH) -> None:
    """Create the table to store ticker statistics."""
    con = duckdb.connect(db_path)
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
            symbol TEXT PRIMARY KEY,
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
        """
    )
    con.close()


def update_ticker_metrics(symbol: str, metrics: dict, db_path: str = DB_PATH) -> None:
    """Upsert metrics for a ticker into the ticker_stats table."""
    con = duckdb.connect(db_path)
    columns = ["symbol"] + list(metrics.keys())
    placeholders = ", ".join(["?"] * len(columns))
    updates = ", ".join([f"{k}=excluded.{k}" for k in metrics.keys()])
    sql = (
        f"INSERT INTO {METRICS_TABLE} ({', '.join(columns)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT(symbol) DO UPDATE SET {updates}"
    )
    con.execute(sql, [symbol] + list(metrics.values()))
    con.close()


def update_last_date(symbol: str, last_date: date, db_path: str = DB_PATH) -> None:
    """Convenience wrapper to record only the last_date for a ticker."""
    update_ticker_metrics(symbol, {"last_date": last_date}, db_path)


def get_tickers_needing_update(db_path: str = DB_PATH) -> List[str]:
    """Return tickers without recent data and not marked bad."""
    ensure_raw_tickers_initialized(db_path)
    init_metrics_table(db_path)
    con = duckdb.connect(db_path)
    safe_lag_date = get_safe_lag_date()
    rows = con.execute(
        f"""
        SELECT r.symbol
        FROM {RAW_TABLE} r
        LEFT JOIN {METRICS_TABLE} m ON r.symbol = m.symbol
        WHERE r.is_bad = FALSE AND (m.last_date IS NULL OR m.last_date < ?)
        ORDER BY m.last_date NULLS FIRST
        """,
        (safe_lag_date,),
    ).fetchall()
    con.close()
    return [r[0] for r in rows]
