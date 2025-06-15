# oqk/price_db.py
import os
from datetime import date
import pandas as pd

TABLE_NAME = "prices"


def get_price_db_path(ticker: str, data_dir: str = "data") -> str:
    return os.path.join(data_dir, f"{ticker}.parquet")


def get_ticker_max_date(ticker: str, data_dir: str = "data") -> date | None:
    """Return the max date from the parquet price file if it exists."""
    file_path = get_price_db_path(ticker, data_dir)
    if not os.path.exists(file_path):
        return None

    try:
        df = pd.read_parquet(file_path, columns=["date"])
        if df.empty:
            return None
        return pd.to_datetime(df["date"]).dt.date.max()
    except Exception:
        return None


def append_price_data(ticker: str, df: pd.DataFrame, data_dir: str = "data") -> None:
    """Append a price DataFrame to the ticker's parquet file, creating it if needed."""
    file_path = get_price_db_path(ticker, data_dir)

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    if os.path.exists(file_path):
        existing = pd.read_parquet(file_path)
        df = pd.concat([existing, df], ignore_index=True)

    df = df.sort_values("date").drop_duplicates("date", keep="last")
    df.to_parquet(file_path, index=False)
