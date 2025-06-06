import marimo

app = marimo.App()

@app.cell
def _():
    import pandas as pd
    import numpy as np
    return pd, np

@app.cell
def _(pd, np):
    dates = pd.date_range('2023-01-01', periods=100)
    data = np.random.randn(100, 3).cumsum(axis=0)
    df = pd.DataFrame(data, index=dates, columns=['AAPL', 'MSFT', 'GOOGL'])
    df
    return df

@app.cell
def _(df):
    df.plot(title='Sample Prices')

if __name__ == '__main__':
    app.run()
