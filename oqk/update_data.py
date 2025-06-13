import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import random

import duckdb
import pandas as pd
import yfinance as yf
from tqdm import tqdm

from .get_tickers import get_all_tickers


def update_ticker_data(tickers: list[str] | None = None, data_dir: str = "data") -> str:
    """Download and store price data for the given tickers."""
    if tickers is None:
        tickers = get_all_tickers()

    random.shuffle(tickers)
    os.makedirs(data_dir, exist_ok=True)

    def update_ticker(ticker: str) -> str:
        try:
            db_path = os.path.join(data_dir, f"{ticker}.duckdb")
            conn = duckdb.connect(db_path)
            table_name = ticker

            today = date.today()

            try:
                result = conn.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()
                max_date = result[0] if result and result[0] else None

                if max_date is not None:
                    if max_date >= today:
                        conn.close()
                        return f"{ticker}: Up-to-date"
                    start_date = max_date + timedelta(days=1)
                    if start_date > today:
                        conn.close()
                        return f"{ticker}: Nothing to fetch"
                else:
                    start_date = None
            except Exception:
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date DATE, close DOUBLE)")
                start_date = None

            df = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)
            if df.empty:
                conn.close()
                return f"{ticker}: No data"

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(filter(None, col)) for col in df.columns]

            close_cols = [col for col in df.columns if col.lower().startswith("close")]
            if not close_cols:
                conn.close()
                return f"{ticker}: No close column"

            close_df = df[close_cols].copy()
            close_df.columns = ["close"]
            close_df.reset_index(inplace=True)

            if "index" in close_df.columns:
                close_df.rename(columns={"index": "date"}, inplace=True)
            elif "Date" in close_df.columns:
                close_df.rename(columns={"Date": "date"}, inplace=True)

            close_df["date"] = pd.to_datetime(close_df["date"]).dt.date

            conn.register("close_df", close_df)
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM close_df")
            conn.close()
            return f"{ticker}: Updated"

        except Exception as e:  # pragma: no cover - network dependent
            return f"{ticker}: Error - {e}"

    max_threads = min(1, len(tickers))
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        results = list(tqdm(executor.map(update_ticker, tickers), total=len(tickers), desc="Updating tickers"))

    for r in results:
        print(r)

    return data_dir


if __name__ == "__main__":
    update_ticker_data()
