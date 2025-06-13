import duckdb

# Connect to the DuckDB file
con = duckdb.connect(database='c:/Users/user/Google Drive/Projects/Electricity Prices/data/tracker.duckdb', read_only=True)

# List all tables in the database
tables = con.execute("SHOW TABLES").fetchall()
table_name = tables[0][0]
num_tables = len(tables)

df = con.execute(f"SELECT * FROM {tables[0][0]}").df()
print(df)

df = con.execute(f"PRAGMA table_info({table_name})").fetchdf()
print(df)

from datetime import datetime

start = datetime(2025, 5, 29, 22, 15)
end = datetime(2025, 6, 12, 22, 15)

delta_minutes = (end - start).total_seconds() / 60
five_min_intervals = int(delta_minutes / 5)

print(f"Total 5-minute intervals between {start} and {end}: {five_min_intervals}")
#Close the connection
con.close()
