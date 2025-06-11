import marimo

__generated_with = "0.13.15"
app = marimo.App(width="full")


@app.cell
def _():
    import yfinance as yf
    import pandas as pd

    # Define your tickers
    tickers = ["REM"]

    # Download historical adjusted close prices
    data = yf.download(tickers, start="2018-01-01", end="2024-12-31")#['Adj Close']

    data
    return (pd,)


@app.cell
def _(pd):

    # Get NASDAQ-listed tickers
    nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep='|')
    nasdaq_tickers = nasdaq['Symbol'].tolist()

    # Get NYSE + others
    nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep='|')
    nyse_tickers = nyse['ACT Symbol'].tolist()

    # Combine and remove test tickers
    all_tickers = list(set(nasdaq_tickers + nyse_tickers))

    all_tickers
    return (all_tickers,)


@app.cell
def _(all_tickers):
    all_tickers
    return


if __name__ == "__main__":
    app.run()
