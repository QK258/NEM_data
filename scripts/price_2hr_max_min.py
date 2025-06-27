import pandas as pd
import duckdb
from datetime import datetime

# Base path with only region-level partitioning
parquet_path = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data\regional_RRP_data\REGIONID=VIC1"

# Load all files for VIC1 and filter by May 2024
df = duckdb.query(f"""
    SELECT *
    FROM parquet_scan('{parquet_path}/*.parquet')
    WHERE SETTLEMENTDATE >= '2025-05-01' AND SETTLEMENTDATE < '2025-06-01'
""").to_df()

# Parse and sort datetime
df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"])
df = df.sort_values("SETTLEMENTDATE")
df["DATE"] = df["SETTLEMENTDATE"].dt.date

# 2-hour rolling average (24 x 5-min intervals)
df["rolling_avg"] = df["RRP"].rolling(window=24, min_periods=1).mean()

# Daily max/min rolling average windows
daily_summary = []
for date, group in df.groupby("DATE"):
    if len(group) < 24:
        continue
    min_row = group.loc[group["rolling_avg"].idxmin()]
    max_row = group.loc[group["rolling_avg"].idxmax()]
    daily_summary.append({
        "date": date,
        "min_start": min_row["SETTLEMENTDATE"],
        "min_2h_avg": min_row["rolling_avg"],
        "max_start": max_row["SETTLEMENTDATE"],
        "max_2h_avg": max_row["rolling_avg"],
        "spread": max_row["rolling_avg"] - min_row["rolling_avg"]
    })

summary_df = pd.DataFrame(daily_summary)
print(summary_df)
# Save the summary DataFrame to a CSV file
summary_df.to_csv("vic1_may_2025_summary.csv", index=False)