# oqk/dim_ticker.py
import os

import pandas as pd

TICKER_CSV_PATH = "seeds/dim_ticker.csv"


def populate_tickers_from_exchange(csv_path: str = TICKER_CSV_PATH) -> None:
    """Download NASDAQ and NYSE tickers and save to CSV if not already present."""
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        if not df.empty and "symbol" in df.columns:
            print("Ticker list already populated.")
            return

    print("Downloading NASDAQ/NYSE tickers...")

    nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep="|")
    nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep="|")

    symbols = set(nasdaq["Symbol"].dropna().tolist() + nyse["ACT Symbol"].dropna().tolist())
    symbols = sorted(t for t in symbols if "test" not in t.lower())

    df = pd.DataFrame({"symbol": symbols})
    df.to_csv(csv_path, index=False)

    print(f"Saved {len(symbols)} tickers to {csv_path}")


def get_tickers_needing_update(csv_path: str = TICKER_CSV_PATH) -> list:
    """Read ticker symbols from CSV and return as list."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Ticker file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if "symbol" not in df.columns:
        raise ValueError(f"'symbol' column missing in {csv_path}")

    return df["symbol"].dropna().tolist()
