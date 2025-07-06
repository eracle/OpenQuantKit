import pandas as pd
import json

# Load CIK mapping from a local JSON file
with open("/data/company_tickers.json", "r") as f:
    data = json.load(f)

# Convert to DataFrame
records = []
for _, entry in data.items():
    records.append({
        "ticker": entry["ticker"],
        "cik": str(entry["cik_str"]).zfill(10),
        "title": entry["title"]
    })

df = pd.DataFrame(records)

# Save as CSV for dbt seed
csv_path = "./dagster/dbt/seeds/dim_cik.csv"
df.to_csv(csv_path, index=False)

print(f"Saved CSV to: {csv_path}")
