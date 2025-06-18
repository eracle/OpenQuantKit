import os
from datetime import timedelta
from typing import Optional

import pandas as pd
import yfinance as yf
from pandas.tseries.offsets import BDay
from tqdm import tqdm

from .dim_ticker import TICKER_CSV_PATH

DATA_DIR = "data/raw_price/"


def get_price_db_path(ticker: str, data_dir: str = DATA_DIR) -> str:
    return os.path.join(data_dir, f"{ticker}.parquet")


def get_ticker_max_date(ticker: str, data_dir: str = DATA_DIR) -> Optional[pd.Timestamp]:
    file_path = get_price_db_path(ticker, data_dir)
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_parquet(file_path, columns=["date"])
        if df.empty:
            return None
        return pd.to_datetime(df["date"]).max()
    except Exception:
        return None


def append_price_data(ticker: str, df: pd.DataFrame, data_dir: str = DATA_DIR) -> None:
    file_path = get_price_db_path(ticker, data_dir)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    if os.path.exists(file_path):
        existing = pd.read_parquet(file_path)
        df = pd.concat([existing, df], ignore_index=True)

    df = df.sort_values("date").drop_duplicates("date", keep="last")
    df.to_parquet(file_path, index=False)


def get_safe_lag_date() -> pd.Timestamp:
    return (pd.Timestamp.today() - BDay(1)).normalize()


def update_ticker(ticker: str, data_dir: str = DATA_DIR) -> Optional[pd.Timestamp]:
    end_date = get_safe_lag_date()
    start_date = None

    try:
        max_date = get_ticker_max_date(ticker, data_dir)
        if max_date and max_date >= end_date:
            print(f"{ticker}: Up to date ({max_date.date()})")
            return max_date
        elif max_date:
            start_date = max_date + timedelta(days=1)
            if start_date > end_date:
                print(f"{ticker}: Nothing to fetch")
                return max_date
    except Exception as e:
        print(f"{ticker}: Error reading data - {e}")
        return None

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

    df = df[["date", "close"]]
    append_price_data(ticker, df, data_dir)

    print(f"{ticker}: Updated to {df['date'].max().date()}")
    return df["date"].max()


def update_all_tickers(csv_path: str = TICKER_CSV_PATH, data_dir: str = DATA_DIR) -> None:
    os.makedirs(data_dir, exist_ok=True)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Ticker list not found: {csv_path}")

    tickers = pd.read_csv(csv_path)["symbol"].dropna().unique().tolist()

    for ticker in tqdm(tickers, desc="Updating prices"):
        update_ticker(ticker, data_dir)


if __name__ == "__main__":
    update_all_tickers()
