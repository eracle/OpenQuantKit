import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from enum import Enum, auto

import duckdb
import pandas as pd
import yfinance as yf
from tqdm import tqdm
from yfinance.exceptions import YFRateLimitError, YFInvalidPeriodError

from .tickers import (
    init_ticker_table,
    get_all_valid_tickers,
    mark_ticker_as_bad,
    update_max_date,
    DB_PATH, TABLE_NAME, get_safe_lag_date
)


class TickerUpdateState(Enum):
    UPDATED = auto()
    UP_TO_DATE = auto()
    FAILED = auto()
    RATE_LIMITED = auto()


class RateLimitException(Exception):
    pass


def update_ticker(ticker: str, data_dir: str = "data") -> tuple[TickerUpdateState, date | None]:
    end_date = get_safe_lag_date()

    try:
        fast_info = yf.Ticker(ticker).fast_info
        if not fast_info or fast_info.get("lastPrice") is None:
            print(f"{ticker}: Invalid or no market data")
            return TickerUpdateState.FAILED, None
    except YFRateLimitError:
        print(f"{ticker}: Hit Yahoo Finance rate limit during validation.")
        return TickerUpdateState.RATE_LIMITED, None
    except Exception as e:
        print(f"{ticker}: Validation failed - {e}")
        return TickerUpdateState.FAILED, None

    db_path = os.path.join(data_dir, f"{ticker}.duckdb")
    conn = duckdb.connect(db_path)

    try:
        result = conn.execute(f"SELECT MAX(date) FROM {TABLE_NAME}").fetchone()
        max_date = result[0] if result and result[0] else None

        if max_date is not None:
            if max_date >= end_date:
                conn.close()
                return TickerUpdateState.UP_TO_DATE, max_date
            start_date = max_date + timedelta(days=1)
            if start_date > end_date:
                conn.close()
                print(f"{ticker}: Nothing to fetch")
                return TickerUpdateState.UP_TO_DATE, max_date
        else:
            start_date = None
    except Exception:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (date DATE, close DOUBLE)")
        start_date = None

    try:
        df = yf.download(ticker, start=start_date, end=end_date + timedelta(days=1),
                         progress=False, auto_adjust=True)
    except YFRateLimitError:
        conn.close()
        print(f"{ticker}: Hit Yahoo Finance rate limit during download.")
        return TickerUpdateState.RATE_LIMITED, None
    except YFInvalidPeriodError as e:
        conn.close()
        print(f"{ticker}: Invalid period during download - {e}")
        return TickerUpdateState.FAILED, None
    except Exception as e:
        conn.close()
        print(f"{ticker}: Download error - {e}")
        return TickerUpdateState.RATE_LIMITED, None

    if df.empty:
        conn.close()
        print(f"{ticker}: No data")
        return TickerUpdateState.FAILED, None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(filter(None, map(str, col))) for col in df.columns]

    close_cols = [col for col in df.columns if col.lower().startswith("close")]
    if not close_cols:
        conn.close()
        print(f"{ticker}: No close column")
        return TickerUpdateState.FAILED, None

    close_df = df[close_cols].copy()
    close_df.columns = ["close"]
    close_df.reset_index(inplace=True)

    if "Date" in close_df.columns:
        close_df.rename(columns={"Date": "date"}, inplace=True)

    close_df["date"] = pd.to_datetime(close_df["date"]).dt.date

    conn.register("close_df", close_df)
    conn.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM close_df")
    conn.close()

    last_date = close_df["date"].max()
    # print(f"{ticker}: Updated to {last_date}")
    return TickerUpdateState.UPDATED, last_date


def handle_ticker_result(ticker: str, state: TickerUpdateState, max_date: date | None) -> None:
    match (state, max_date):
        case (TickerUpdateState.UPDATED, d) if d is not None:
            update_max_date(ticker, d, DB_PATH)
        case (TickerUpdateState.FAILED, _):
            mark_ticker_as_bad(ticker, DB_PATH)
        case _:
            pass  # UP_TO_DATE or RATE_LIMITED


def update_ticker_sequential(tickers: list[str], data_dir: str) -> None:
    for ticker in tqdm(tickers, desc="Updating tickers (sequential)"):
        state, max_date = update_ticker(ticker, data_dir)
        match state:
            case TickerUpdateState.RATE_LIMITED:
                raise RateLimitException("Yahoo Finance rate limit reached.")
            case _:
                handle_ticker_result(ticker, state, max_date)


def flush_updates(
        bad_tickers: list[str],
        updated_tickers: list[tuple[str, date]]
) -> None:
    for t, d in updated_tickers:
        handle_ticker_result(t, TickerUpdateState.UPDATED, d)
    for t in bad_tickers:
        handle_ticker_result(t, TickerUpdateState.FAILED, None)
    updated_tickers.clear()
    bad_tickers.clear()


def update_ticker_parallel(tickers: list[str], data_dir: str, max_workers: int, batch_size: int = 16) -> None:
    def run_update(ticker: str) -> tuple[str, TickerUpdateState, date | None]:
        state, max_date = update_ticker(ticker, data_dir)
        return ticker, state, max_date

    bad_tickers = []
    updated_tickers = []
    processed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_update, ticker): ticker for ticker in tickers}
        try:
            for future in tqdm(as_completed(futures), total=len(futures), desc="Updating tickers (parallel)"):
                ticker, state, max_date = future.result()

                match (state, max_date):
                    case (TickerUpdateState.RATE_LIMITED, _):
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise RateLimitException(f"Rate limit hit while processing {ticker}")
                    case (TickerUpdateState.UPDATED, d) if d is not None:
                        updated_tickers.append((ticker, d))
                    case (TickerUpdateState.FAILED, _):
                        bad_tickers.append(ticker)
                    case _:
                        pass  # UP_TO_DATE

                processed_count += 1
                if processed_count % batch_size == 0:
                    flush_updates(bad_tickers, updated_tickers)

        except RateLimitException as e:
            print(f"❌ Aborting: {e}")
            flush_updates(bad_tickers, updated_tickers)
            return

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

    try:
        if parallel:
            update_ticker_parallel(tickers, data_dir, max_workers=1, batch_size=1)
        else:
            update_ticker_sequential(tickers, data_dir)
    except RateLimitException:
        print("⚠️  Yahoo Finance rate limit reached. Stopped execution.")

    return data_dir


if __name__ == "__main__":
    update_ticker_data(parallel=True)
