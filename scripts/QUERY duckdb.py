import duckdb
import pandas as pd

# Connect to DuckDB
db_path = r"C:/Users/user/Google Drive/Projects/Electricity Prices/data/price_tracker.duckdb"
con = duckdb.connect(database=db_path)

# Query for VIC1: find 2-hour rolling average (24 x 5-min), min & max per day
query = """
WITH price_data AS (
    SELECT
        settlementdate,
        regionid,
        rrp,
        DATE_TRUNC('month', settlementdate) AS month
    FROM read_parquet('C:/Users/user/Google Drive/Projects/Electricity Prices/data/monthly_price_data/*/*.parquet')
    WHERE regionid = 'VIC1'
),
rolling_avg AS (
    SELECT
        settlementdate,
        regionid,
        month,
        AVG(rrp) OVER (
            PARTITION BY regionid ORDER BY settlementdate
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS avg_rrp_2hr
    FROM price_data
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY avg_rrp_2hr ASC) AS row_min,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY avg_rrp_2hr DESC) AS row_max
    FROM rolling_avg
)
SELECT *
FROM ranked
WHERE row_min = 1 OR row_max = 1
ORDER BY month, row_min;
"""
# This script connects to a DuckDB database and executes a query to find the
# minimum and maximum 2-hour rolling average prices for the RRP in the VIC1 region


df = con.execute(query).fetchdf()

# Preview and export
print(df.head())
df.to_csv("vic_2hr_bess_min_max_price_by_day.csv", index=False)
