import duckdb
import pandas as pd

# Connect to DuckDB
db_path = r"C:/Users/user/Google Drive/Projects/Electricity Prices/data/price_tracker.duckdb"
con = duckdb.connect(database=db_path)

# Query for VIC1: find 2-hour rolling average (24 x 5-min), min & max per day
query = """
SELECT
    REGIONID,
    ROUND(AVG(RRP), 2) AS avg_price
FROM
    read_parquet('C:/Users/user/Google Drive/Projects/Electricity Prices/data/monthly_price_data/*/*.parquet')
WHERE
    REGIONID ='TAS1'
    AND SETTLEMENTDATE >= '2024-06-01'
    AND SETTLEMENTDATE < '2025-05-31'
GROUP BY
    REGIONID
ORDER BY
    REGIONID
"""
# This script connects to a DuckDB database and executes a query to find the average
# spot price for each region, excluding TAS1, over a specified date range.

df = con.execute(query).fetchdf()

# Preview and export
print(df.head())
df.to_csv("average_price.csv", index=False)
