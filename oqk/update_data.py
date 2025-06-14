import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

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


def update_ticker(ticker: str, data_dir: str = "data") -> tuple[bool, date | None]:
    today = date.today()
    end_date = today - timedelta(days=1)

    try:
        fast_info = yf.Ticker(ticker).fast_info
        if not fast_info or fast_info.get("lastPrice") is None:
            print(f"{ticker}: Invalid or no market data")
            return False, None
    except Exception as e:
        print(f"{ticker}: Validation failed - {e}")
        return False, None

    db_path = os.path.join(data_dir, f"{ticker}.duckdb")
    conn = duckdb.connect(db_path)
    table_name = ticker

    try:
        result = conn.execute(f"SELECT MAX(date) FROM {table_name}").fetchone()
        max_date = result[0] if result and result[0] else None

        if max_date is not None:
            if max_date >= end_date:
                conn.close()
                print(f"{ticker}: Up-to-date")
                return True, max_date
            start_date = max_date + timedelta(days=1)
            if start_date > end_date:
                conn.close()
                print(f"{ticker}: Nothing to fetch")
                return True, max_date
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
        print(f"{ticker}: Download error - {e}")
        return False, None

    if df.empty:
        conn.close()
        print(f"{ticker}: No data")
        return False, None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(filter(None, map(str, col))) for col in df.columns]

    close_cols = [col for col in df.columns if col.lower().startswith("close")]
    if not close_cols:
        conn.close()
        print(f"{ticker}: No close column")
        return False, None

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
    print(f"{ticker}: Updated to {last_date}")
    return True, last_date


def handle_ticker_result(ticker: str, success: bool, max_date: date | None) -> None:
    if success and max_date:
        update_max_date(ticker, max_date, DB_PATH)
    elif not success:
        mark_ticker_as_bad(ticker, DB_PATH)


def update_ticker_sequential(tickers: list[str], data_dir: str) -> None:
    for ticker in tqdm(tickers, desc="Updating tickers (sequential)"):
        success, max_date = update_ticker(ticker, data_dir)
        handle_ticker_result(ticker, success, max_date)


def flush_updates(bad_tickers: list[str], updated_tickers: list[tuple[str, date]]) -> None:
    for t, d in updated_tickers:
        handle_ticker_result(t, True, d)
    for t in bad_tickers:
        handle_ticker_result(t, False, None)
    updated_tickers.clear()
    bad_tickers.clear()


def update_ticker_parallel(tickers: list[str], data_dir: str, max_workers: int, batch_size: int = 32) -> None:
    def run_update(ticker: str) -> tuple[str, bool, date | None]:
        success, max_date = update_ticker(ticker, data_dir)
        return ticker, success, max_date

    bad_tickers = []
    updated_tickers = []
    processed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_update, ticker): ticker for ticker in tickers}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Updating tickers (parallel)"):
            ticker, success, max_date = future.result()
            processed_count += 1

            if success and max_date:
                updated_tickers.append((ticker, max_date))
            elif not success:
                bad_tickers.append(ticker)

            if processed_count % batch_size == 0:
                flush_updates(bad_tickers, updated_tickers)

    flush_updates(bad_tickers, updated_tickers)


def update_ticker_data(
        tickers: list[str] | None = None,
        data_dir: str = "data",
        max_workers: int = 4,
        parallel: bool = True
) -> str:
    init_ticker_table()

    if tickers is None:
        tickers = get_all_valid_tickers(DB_PATH)

    os.makedirs(data_dir, exist_ok=True)

    if parallel:
        update_ticker_parallel(tickers, data_dir, max_workers)
    else:
        update_ticker_sequential(tickers, data_dir)

    return data_dir


if __name__ == "__main__":
    update_ticker_data(parallel=True)
