# oqk/update_data.py
import os
from datetime import date, timedelta
from enum import Enum, auto
from typing import Optional

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from yfinance.exceptions import YFRateLimitError, YFInvalidPeriodError

from .price_db import get_ticker_max_date, append_price_data
from .raw_ticker_db import (
    init_raw_ticker_table,
    mark_ticker_as_bad,
    get_safe_lag_date,
)
from .ticker_metrics_db import (
    init_metrics_table,
    update_ticker_metrics,
    update_last_date,
    get_tickers_needing_update,
)
from .ticker_metrics import compute_ticker_metrics


class TickerUpdateState(Enum):
    UPDATED = auto()
    UP_TO_DATE = auto()
    FAILED = auto()
    RATE_LIMITED = auto()


class RateLimitException(Exception):
    pass


def update_ticker(ticker: str, data_dir: str = "data") -> tuple[TickerUpdateState, Optional[date]]:
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

    try:
        max_date = get_ticker_max_date(ticker, data_dir)

        if max_date is not None:
            if max_date >= end_date:
                return TickerUpdateState.UP_TO_DATE, max_date
            start_date = max_date + timedelta(days=1)
            if start_date > end_date:
                print(f"{ticker}: Nothing to fetch")
                return TickerUpdateState.UP_TO_DATE, max_date
        else:
            start_date = None
    except Exception as e:
        print(f"{ticker}: Error reading or initializing DB - {e}")
        return TickerUpdateState.FAILED, None

    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date + timedelta(days=1),
            progress=False,
            auto_adjust=True,
        )
    except YFRateLimitError:
        print(f"{ticker}: Hit Yahoo Finance rate limit during download.")
        return TickerUpdateState.RATE_LIMITED, None
    except YFInvalidPeriodError as e:
        print(f"{ticker}: Invalid period during download - {e}")
        return TickerUpdateState.FAILED, None
    except Exception as e:
        print(f"{ticker}: Download error - {e}")
        return TickerUpdateState.RATE_LIMITED, None

    if df.empty:
        print(f"{ticker}: No data")
        return TickerUpdateState.FAILED, None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(filter(None, map(str, col))) for col in df.columns]

    close_cols = [col for col in df.columns if col.lower().startswith("close")]
    if not close_cols:
        print(f"{ticker}: No close column")
        return TickerUpdateState.FAILED, None

    close_df = df[close_cols].copy()
    close_df.columns = ["close"]
    close_df.reset_index(inplace=True)

    if "Date" in close_df.columns:
        close_df.rename(columns={"Date": "date"}, inplace=True)

    close_df["date"] = pd.to_datetime(close_df["date"]).dt.date

    append_price_data(ticker, close_df, data_dir)

    last_date = close_df["date"].max()
    metrics = compute_ticker_metrics(close_df)

    if metrics:
        update_ticker_metrics(ticker, metrics)

    return TickerUpdateState.UPDATED, last_date


def handle_ticker_result(ticker: str, state: TickerUpdateState, last_date: Optional[date]) -> None:
    match (state, last_date):
        case (TickerUpdateState.UPDATED, d) if d is not None:
            update_last_date(ticker, d)
        case (TickerUpdateState.FAILED, _):
            mark_ticker_as_bad(ticker)
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


def update_ticker_data(
    tickers: Optional[list[str]] = None,
    data_dir: str = "data",
) -> str:
    init_raw_ticker_table()
    init_metrics_table()

    if tickers is None:
        tickers = get_tickers_needing_update()

    os.makedirs(data_dir, exist_ok=True)
    update_ticker_sequential(tickers, data_dir)

    return data_dir


if __name__ == "__main__":
    update_ticker_data()
