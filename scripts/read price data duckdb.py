import duckdb

# Connect to DuckDB (or use :memory: if temporary)
con = duckdb.connect()

# Query all Parquet files in your filtered folder
df = con.execute("""
    SELECT * 
    FROM parquet_scan('C:/Users/user/Google Drive/Projects/Electricity Prices/data/filtered_price_data/**/*.parquet') 
    LIMIT 100
""").fetchdf()

print(df.head())

print(df.columns.tolist())