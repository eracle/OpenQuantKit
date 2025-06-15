# 🧠 OpenQuantKit

**OpenQuantKit** is an open-source pipeline for quantitative stock analysis and portfolio management. Designed with notebooks in mind, it enables a fully automated and reproducible workflow from ticker selection to portfolio rebalancing.

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
