# # oqk/ticker_downloader.py

import pandas as pd
import duckdb

DB_PATH = "tickers.duckdb"


def populate_tickers_from_exchange(db_path: str = DB_PATH) -> None:
    """Download NASDAQ and NYSE tickers and insert them into the DB."""
    con = duckdb.connect(db_path)

    count = con.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
    if count > 0:
        con.close()
        return  # Already populated

    print("Downloading NASDAQ/NYSE tickers...")

    nasdaq = pd.read_csv(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
        sep="|"
    )
    nyse = pd.read_csv(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt",
        sep="|"
    )

    symbols = set(nasdaq["Symbol"].dropna().tolist() + nyse["ACT Symbol"].dropna().tolist())
    symbols = sorted(t for t in symbols if "test" not in t.lower())

    con.executemany(
        "INSERT INTO tickers (symbol) VALUES (?)",
        [(s,) for s in symbols]
    )
    con.close()

    print(f"Saved {len(symbols)} tickers to DuckDB")
