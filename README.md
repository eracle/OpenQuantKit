# 🧠 OpenQuantKit

The open-source data pipeline toolkit for modern quant research.
Clean data. Modular pipelines. Real financial signals.

📈 What is Open Quant Kit?
Open Quant Kit (OQK) is an open-source, modular data pipeline framework designed for developers, analysts, and researchers who want to work with financial data the right way.

It connects real-time and historical market data to a clean, extensible analytics stack using tools like Dagster, dbt, and Python. From ingestion to transformation to dashboard-ready metrics — everything is reproducible, inspectable, and built for scale.

🔧 Features
✅ Modular pipelines powered by Dagster

✅ Clean transformations using dbt

✅ Support for price data, earnings, filings, and more

✅ Easy local setup with DuckDB / CSV

✅ Plug-in ready architecture for indicators, signals, and strategies

✅ Compatible with Streamlit, Jupyter, and backtesting frameworks

✅ Ready for cloud or local deployment

💡 Why Use It?
Most quant projects start with messy CSVs and brittle scripts.
Open Quant Kit gives you a clean slate, structured flow, and modular power — without the enterprise price tag or bloat.

Whether you're:

🧑‍💻 Building your own quant system

🎓 Working on a finance/data science thesis

🧠 Testing new indicators

🚀 Launching a new fintech tool

...OQK gives you a real data foundation with zero vendor lock-in.

---

## 🚀 Features

1. **Input Tickers**
   - Define custom ticker lists based on sectors, indices, or personal criteria.

2. **Efficient Data Loading**
   - Automatically fetch and cache only the most recent stock data.
   - Prevents redundant downloads to optimize performance.

3. **Data Validation**
   - Detect and report missing or anomalous values.
   - Ensure a sufficient historical window for accurate analysis.

4. **Data Fixing**
   - Interpolate missing data points.
   - Identify and correct outliers.
   - Optionally drop unreliable tickers.

5. **Hyperparameter Search & Tuning**
   - Perform automated hyperparameter optimization (e.g., Prophet).
   - Store configurations and results for auditability and reuse.

6. **Final Model Training**
   - Train forecasting models on cleaned, validated datasets.

7. **Forecasting**
   - Use Prophet to forecast future stock behavior.
   - Identify stocks with a low probability of underperforming over a defined horizon (e.g., 2 years).

8. **Secondary Ticker Selection & Portfolio Construction**
   - **Step 1: Forecast-Guided Selection**  
     Use probabilistic forecasts to identify a shortlist of "winning" stocks—those with high confidence in outperforming their current price over the investment horizon.
   - **Step 2: Greedy Portfolio Expansion**  
     Apply a **greedy algorithm** to iteratively build a diversified portfolio.  
     At each step, add the stock that most reduces overall portfolio variance.  
     Favor assets that are negatively correlated (or weakly correlated) with existing holdings.
   - **Step 3: Hierarchical Portfolio Construction**  
     Build the portfolio bottom-up, balancing risk and return.  
     Prioritize anti-correlated assets to increase robustness and minimize drawdown potential.

9. **Portfolio Balancing**
   - Allocate weights based on model insights and risk minimization strategies.

10. **Portfolio Difference Analysis**
    - Compare current allocations to target allocations.
    - Identify required trades to rebalance effectively.

11. **Trade Logging**
    - Maintain a history of executed trades and portfolio states for backtesting and review.

---

## ⚡ Quickstart

Install the dependencies and update price data locally:

```bash
pip install -r requirements.txt
python -m oqk.update_data
```

Or run the same command inside Docker:

```bash
make compose
```

This builds the container and runs the data update script.

## \ud83d\udd2e Tests

Run the unit tests with:

```bash
make test
```
