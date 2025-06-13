import os
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import pandas as pd
import yfinance as yf
from tqdm import tqdm

from .tickers import (
    init_ticker_table,
    get_all_valid_tickers,
    mark_ticker_as_bad,
    update_max_date,
)

DB_PATH = "tickers.duckdb"


def update_ticker(ticker: str, data_dir: str = "data") -> str:
    today = date.today()
    end_date = today - timedelta(days=1)

    try:
        fast_info = yf.Ticker(ticker).fast_info
        if not fast_info or fast_info.get("lastPrice") is None:
            mark_ticker_as_bad(ticker, DB_PATH)
            return f"{ticker}: Invalid or no market data"
    except Exception as e:
        mark_ticker_as_bad(ticker, DB_PATH)
        return f"{ticker}: Validation failed - {e}"

    db_path = os.path.join(data_dir, f"{ticker}.duckdb")
    conn = duckdb.connect(db_path)
    table_name = ticker

    try:
        result = conn.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()
        max_date = result[0] if result and result[0] else None

        if max_date is not None:
            if max_date >= end_date:
                conn.close()
                update_max_date(ticker, str(max_date), DB_PATH)
                return f"{ticker}: Up-to-date"
            start_date = max_date + timedelta(days=1)
            if start_date > end_date:
                conn.close()
                update_max_date(ticker, str(max_date), DB_PATH)
                return f"{ticker}: Nothing to fetch"
        else:
            start_date = None
    except Exception:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date DATE, close DOUBLE)")
        start_date = None

    try:
        df = yf.download(ticker, start=start_date, end=end_date + timedelta(days=1),
                         progress=False, auto_adjust=True)
    except Exception as e:
        conn.close()
        mark_ticker_as_bad(ticker, DB_PATH)
        return f"{ticker}: Download error - {e}"

    if df.empty:
        conn.close()
        return f"{ticker}: No data"

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(filter(None, map(str, col))) for col in df.columns]

    close_cols = [col for col in df.columns if col.lower().startswith("close")]
    if not close_cols:
        conn.close()
        return f"{ticker}: No close column"

    close_df = df[close_cols].copy()
    close_df.columns = ["close"]
    close_df.reset_index(inplace=True)

    if "Date" in close_df.columns:
        close_df.rename(columns={"Date": "date"}, inplace=True)

    close_df["date"] = pd.to_datetime(close_df["date"]).dt.date

    conn.register("close_df", close_df)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM close_df")
    conn.close()

    last_date = close_df["date"].max()
    update_max_date(ticker, str(last_date), DB_PATH)

    return f"{ticker}: Updated to {last_date}"


def update_ticker_data(tickers: list[str] | None = None, data_dir: str = "data", max_workers: int = 32) -> str:
    init_ticker_table()

    if tickers is None:
        tickers = get_all_valid_tickers(DB_PATH)

    os.makedirs(data_dir, exist_ok=True)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_ticker, ticker, data_dir): ticker for ticker in tickers}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Updating tickers"):
            result = future.result()
            # print(result)
            results.append(result)

    return data_dir


if __name__ == "__main__":
    update_ticker_data()
