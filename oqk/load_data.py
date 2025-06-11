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
    return os, pd, tickers


@app.cell
def _():
    try:
        import pyarrow.parquet
    except ImportError:
        raise ImportError(
            "❌ Missing dependency: `pyarrow` is required to read/write Parquet files.\n"
            "💡 Install it with:\n"
            "   pip install pyarrow\n"
            "   or\n"
            "   conda install pyarrow -c conda-forge"
        )

    return


@app.cell
def _(os, pd, tickers):
    from datetime import timedelta
    import yfinance as yf
    from tqdm.notebook import tqdm


    parquet_dir = "parquet_cache"
    os.makedirs(parquet_dir, exist_ok=True)

    for ticker in tqdm(tickers, desc="Fetching to Parquet"):
        try:
            path = f"{parquet_dir}/{ticker}.parquet"

            # Read last date if exists
            if os.path.exists(path):
                existing_df = pd.read_parquet(path)
                last_date = existing_df["Date"].max()
                start_date = pd.to_datetime(last_date)
            else:
                existing_df = None
                start_date = None

            df = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)
            if df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(filter(None, col)) for col in df.columns]
            close_cols = [col for col in df.columns if col.lower().startswith("close")]
            if not close_cols:
                continue

            close_df = df[close_cols].copy()
            close_df.columns = ['Close']
            close_df.reset_index(inplace=True)
            close_df.rename(columns={'index': 'Date'}, inplace=True)

            if existing_df is not None:
                full_df = pd.concat([existing_df, close_df], ignore_index=True).drop_duplicates(subset=["Date"])
            else:
                full_df = close_df

            full_df.to_parquet(path, index=False)
        except Exception as e:
            print(type(e))
            print(f"Error with {ticker}: {e}")

    return


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
