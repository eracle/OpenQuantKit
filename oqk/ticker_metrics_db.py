from typing import List

import duckdb
import pandas as pd

RAW_TABLE = "raw_tickers"
METRICS_TABLE = "raw_metrics"


def mark_ticker_as_bad(symbol: str) -> None:
    """Mark a ticker as bad in the raw_tickers table."""
    print(f"marked ticket as bad {symbol}")


def init_metrics_table(db_path) -> None:
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


def get_tickers_needing_update(db_path) -> List[str]:
    """Return tickers."""
    tickers = pd.read_csv(db_path)
    return tickers['symbol'].tolist()
