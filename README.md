# 🧠 OpenQuantKit

The open-source data pipeline toolkit for modern quant research.  
**Clean data. Modular pipelines. Real financial signals.**

---

## 📈 What is OpenQuantKit?

**OpenQuantKit (OQK)** is a modular data pipeline framework designed for developers, analysts, and researchers working with financial data.  
It connects historical and real-time market data to a clean, scalable analytics stack — using **Dagster**, **dbt**, and **PostgreSQL**.

From ingestion to transformation to dashboard-ready metrics — everything is reproducible, inspectable, and cloud-ready.

---

## 🔧 Features

✅ Modular pipelines powered by **Dagster**  
✅ Clean transformations using **dbt**  
✅ PostgreSQL-first architecture — no DuckDB  
✅ Fast ingestion from Yahoo Finance via `yfinance`  
✅ Built-in quality metrics for tickers  
✅ Plug-in ready for signals, strategies, and models  
✅ Ready for local or cloud deployment  
✅ Works with **Streamlit**, **Jupyter**, and **backtesting** frameworks

---

## 💡 Why Use It?

Most quant research starts with messy CSVs and ad-hoc scripts.

**OpenQuantKit** gives you:

- A clean, SQL-based foundation
- Transparent, testable data transformations
- Production-grade pipelines
- No vendor lock-in

Whether you're:

- 🧑‍💻 Building a personal quant system  
- 🎓 Working on a research thesis  
- 🧠 Experimenting with alpha factors  
- 🚀 Launching a new fintech prototype  

...OQK gives you structure without sacrificing flexibility.

---

## ⚡ Quickstart (PostgreSQL)

### 1. Build and run the stack

```bash
make build        # Build containers
make postgres     # Start PostgreSQL
make dagster      # Start Dagster + dbt
```

### 2. Ingest raw price data

To fetch all available price data from Yahoo Finance and store in PostgreSQL:

```bash
docker compose run --rm dagster python -m open_quant_kit.raw.raw_price
```

This reads tickers from `dim_ticker`, downloads full price history, and stores it in the `raw_price` table.

### 3. Run dbt models

To compute data quality metrics:

```bash
docker compose run --rm dagster dbt build --select fct_ticker_data_quality
```

The model will create or update a table with per-ticker statistics such as gaps, duplicates, volatility, and completeness.

---

## 📚 Components Overview

### 🔄 Ingestion
- `raw_price.py` — Fetches historical price data from Yahoo Finance
- Modular, resumable, and ticker-aware

### 📊 Transformation
- `fct_ticker_data_quality.sql` — dbt model computing:
  - Data duration and coverage
  - Largest gaps and completeness
  - Volatility and recentness checks

### 🛠️ Resources
- Dagster orchestrates asset materialization and scheduling
- dbt handles transformations declaratively in SQL
- PostgreSQL stores all raw + modeled data

---

## 🤝 Contributing

PRs, issues, and ideas welcome.  
This project is early-stage and evolving. Feedback makes it better!

---

## 📄 License

MIT
