import marimo

__generated_with = "0.13.15"
app = marimo.App(width="full")


@app.cell
def _():
    import pandas as pd
    import os

    def get_all_tickers(file_path="tickers.txt"):
        if os.path.exists(file_path):
            print(f"Reading tickers from {file_path}")
            with open(file_path, "r") as f:
                tickers = [line.strip() for line in f if line.strip()]
            return tickers

        print(f"{file_path} not found. Downloading tickers...")

        # Download NASDAQ-listed tickers
        nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep="|")
        nasdaq_tickers = nasdaq["Symbol"].dropna().tolist()

        # Download NYSE and other-listed tickers
        nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep="|")
        nyse_tickers = nyse["ACT Symbol"].dropna().tolist()

        # Combine and clean
        all_tickers = list(set(nasdaq_tickers + nyse_tickers))
        all_tickers = [t for t in all_tickers if "test" not in t.lower()]

        # Save to file
        with open(file_path, "w") as f:
            for ticker in sorted(all_tickers):
                f.write(f"{ticker}\n")

        print(f"Saved {len(all_tickers)} tickers to {file_path}")
        return sorted(all_tickers)

    # Usage
    tickers = get_all_tickers()  # [:2]
    tickers
    return os, pd, tickers


@app.cell
def _(os, pd, tickers):
    from datetime import timedelta, date
    import yfinance as yf
    import duckdb
    from concurrent.futures import ThreadPoolExecutor
    from tqdm.notebook import tqdm

    import random

    random.shuffle(tickers)
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)

    def update_ticker(ticker):
        try:
            db_path = os.path.join(data_dir, f"{ticker}.duckdb")
            conn = duckdb.connect(db_path)
            table_name = ticker  # no quoting needed in per-file DB

            today = date.today()

            # Try to get max date
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
                    start_date = None  # fetch full history

            except Exception:
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date DATE, close DOUBLE)")
                start_date = None  # new table → fetch full history

            # Download
            print(start_date)
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

        except Exception as e:
            return f"{ticker}: Error - {e}"

    # --- Main Execution ---
    max_threads = min(1, len(tickers))
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        results = list(tqdm(executor.map(update_ticker, tickers), total=len(tickers), desc="Updating tickers"))

    for r in results:
        print(r)

    return data_dir, duckdb


@app.cell
def _(data_dir, duckdb, os):


    # Get the first .duckdb file in the directory (sorted alphabetically)
    duckdb_files = sorted([f for f in os.listdir(data_dir) if f.endswith(".duckdb")])
    if not duckdb_files:
        raise FileNotFoundError("No DuckDB files found in the 'data' directory.")

    first_file = duckdb_files[100]
    db_path = os.path.join(data_dir, first_file)
    table_name = os.path.splitext(first_file)[0]  # table name = ticker = filename without extension

    # Connect and read data
    conn = duckdb.connect(db_path)
    df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    #conn.close()

    # Display the first few rows
    df

    return


if __name__ == "__main__":
    app.run()
