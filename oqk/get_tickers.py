import os
import pandas as pd


def get_all_tickers(file_path: str = "tickers.txt") -> list[str]:
    """Return a list of tickers, downloading them if needed."""
    if os.path.exists(file_path):
        print(f"Reading tickers from {file_path}")
        with open(file_path, "r") as f:
            return [line.strip() for line in f if line.strip()]

    print(f"{file_path} not found. Downloading tickers...")

    nasdaq = pd.read_csv(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
        sep="|",
    )
    nasdaq_tickers = nasdaq["Symbol"].dropna().tolist()

    nyse = pd.read_csv(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt",
        sep="|",
    )
    nyse_tickers = nyse["ACT Symbol"].dropna().tolist()

    all_tickers = list(set(nasdaq_tickers + nyse_tickers))
    all_tickers = [t for t in all_tickers if "test" not in t.lower()]

    with open(file_path, "w") as f:
        for ticker in sorted(all_tickers):
            f.write(f"{ticker}\n")

    print(f"Saved {len(all_tickers)} tickers to {file_path}")
    return sorted(all_tickers)


if __name__ == "__main__":
    tickers = get_all_tickers()
    print(f"Loaded {len(tickers)} tickers")
