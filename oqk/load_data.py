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
    tickers = get_all_tickers()#[:2]
    tickers
    return pd, tickers


@app.cell
def _(pd, tickers):
    from datetime import timedelta
    import yfinance as yf
    import duckdb
    from tqdm.notebook import tqdm


    db_path = "stock_data.duckdb"

    conn = duckdb.connect(db_path)

    for ticker in tqdm(tickers, desc="Updating Close Prices"):
        try:
            # Step 1: Get last date from DB if exists
            try:
                result = conn.execute(f"SELECT MAX(Date) FROM '{ticker}'").fetchone()
                start_date = result[0] + timedelta(days=1) if result and result[0] else None
            except Exception:
                start_date = None  # Table doesn't exist yet

            # Step 2: Fetch new data
            df = yf.download(ticker, start=start_date, progress=False)
            if df.empty:
                continue

            # Flatten columns if MultiIndex
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(filter(None, col)) for col in df.columns]

            # Select the Close column
            close_cols = [col for col in df.columns if col.lower().startswith("close")]
            if not close_cols:
                continue
            close_df = df[close_cols].copy()
            close_df.columns = ['Close']

            # Reset index to keep Date as a column
            close_df.reset_index(inplace=True)
            close_df.rename(columns={'index': 'Date'}, inplace=True)

            # Step 3: Save to DuckDB
            conn.execute(f"CREATE TABLE IF NOT EXISTS '{ticker}' AS SELECT * FROM close_df LIMIT 0")
            conn.execute(f"INSERT INTO '{ticker}' SELECT * FROM close_df")

        except Exception as e:
            print(f"Failed to update {ticker}: {e}")


    return (conn,)


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(A, conn, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM A order by Date desc
        """,
        engine=conn
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
