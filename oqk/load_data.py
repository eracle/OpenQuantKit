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
        nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep='|')
        nasdaq_tickers = nasdaq['Symbol'].dropna().tolist()

        # Download NYSE and other-listed tickers
        nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep='|')
        nyse_tickers = nyse['ACT Symbol'].dropna().tolist()

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
    tickers = get_all_tickers()[:2]
    tickers
    return os, tickers


@app.cell
def _(os, tickers):
    import yfinance as yf
    import duckdb
    from tqdm import tqdm

    def fetch_and_store_to_duckdb(tickers, db_path="stock_data.duckdb", start="2015-01-01", end=None):
        if os.path.exists(db_path):
            print(f"Using existing DuckDB at {db_path}")
        else:
            print(f"Creating new DuckDB at {db_path}")

        conn = duckdb.connect(db_path)

        for ticker in tqdm(tickers, desc="Downloading & storing"):
            try:
                df = yf.download(ticker, start=start, end=end, progress=False)
                if df.empty:
                    continue

                df.reset_index(inplace=True)
                df["Ticker"] = ticker

                # Store in DuckDB: one table per ticker
                conn.execute(f"CREATE OR REPLACE TABLE '{ticker}' AS SELECT * FROM df")
            except Exception as e:
                print(f"Failed to fetch {ticker}: {e}")

        conn.close()
        print(f"Data saved to {db_path}")

    # Usage (after loading tickers)
    fetch_and_store_to_duckdb(tickers)

    return


@app.cell
def _():
    # make the file 
    return


if __name__ == "__main__":
    app.run()
