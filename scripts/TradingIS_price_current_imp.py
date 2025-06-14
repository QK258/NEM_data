import os
import zipfile
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from bs4 import BeautifulSoup
from datetime import datetime

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting update...")


# --- Paths and URLs ---
BASE_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data"
ZIP_DIR = os.path.join(BASE_DIR, "TradingIS_zips")
CSV_DIR = os.path.join(BASE_DIR, "TradingIS_csvs")
PARQUET_DIR = os.path.join(BASE_DIR, "filtered_price_data")
DB_PATH = os.path.join(BASE_DIR, "tracker.duckdb")
URL = "https://nemweb.com.au/Reports/CURRENT/TradingIS_Reports/"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

# --- Make directories ---
os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

# --- Price columns (manually defined) ---
PRICE_COLUMNS = [
    "SETTLEMENTDATE", "RUNNO", "REGIONID", "PERIODID", "RRP", "EEP", "INVALIDFLAG", "LASTCHANGED",
    "ROP", "RAISE6SECRRP", "RAISE6SECROP", "RAISE60SECRRP", "RAISE60SECROP", "RAISE5MINRRP", "RAISE5MINROP",
    "RAISEREGRRP", "RAISEREGROP", "LOWER6SECRRP", "LOWER6SECROP", "LOWER60SECRRP", "LOWER60SECROP",
    "LOWER5MINRRP", "LOWER5MINROP", "LOWERREGRRP", "LOWERREGROP", "RAISE1SECRRP", "RAISE1SECROP",
    "LOWER1SECRRP", "LOWER1SECROP", "PRICE_STATUS"
]

# --- Start DuckDB tracker ---
con = duckdb.connect(DB_PATH)
con.execute("CREATE TABLE IF NOT EXISTS processed_files (filename TEXT PRIMARY KEY)")
existing_files = set(x[0] for x in con.execute("SELECT filename FROM processed_files").fetchall())

# --- Get list of ZIP links ---
print("Scraping zip links...")
soup = BeautifulSoup(requests.get(URL, headers=HEADERS).text, "html.parser")
zip_links = [a['href'] for a in soup.find_all("a", href=True) if a['href'].endswith(".zip")]

# --- Download and process each ZIP ---
for link in zip_links:
    filename = link.split("/")[-1]
    if filename in existing_files:
        print(f"Skipping {filename}, already processed.")
        continue

    zip_path = os.path.join(ZIP_DIR, filename)
    if not os.path.exists(zip_path):
        print(f"Downloading {filename}")
        try:
            r = requests.get(URL + filename, headers=HEADERS)
            with open(zip_path, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"Download failed: {e}")
            continue

    # Extract ZIP
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                csv_path = os.path.join(CSV_DIR, member)
                if not os.path.exists(csv_path):
                    zf.extract(member, CSV_DIR)

                print(f"Processing {member}")
                price_rows = []
                with open(csv_path, 'r', encoding='utf-8') as file:
                    for line in file:
                        if line.startswith("D,TRADING,PRICE"):
                            price_rows.append(line.strip().split(','))

                if not price_rows:
                    print(f"No PRICE data in {member}")
                    continue

                # Convert to DataFrame
                df = pd.DataFrame(price_rows, columns=['prefix1', 'prefix2', 'block', 'version'] + PRICE_COLUMNS)
                df = df.drop(columns=['prefix1', 'prefix2', 'block', 'version'])

                df["filename"] = filename
                try:
                    df["trading_date"] = pd.to_datetime(
                        df["SETTLEMENTDATE"].str.strip('"'),
                        format="%Y/%m/%d %H:%M:%S"
                    ).dt.date.astype(str)
                except Exception as e:
                    print(f"Date parse error: {e}")
                    continue

                # Save as partitioned parquet
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(table, root_path=PARQUET_DIR, partition_cols=["trading_date"])

    except Exception as e:
        print(f"Failed to process {filename}: {e}")
        continue

    # Mark as processed
    con.execute("INSERT OR IGNORE INTO processed_files VALUES (?)", [filename])
    print(f"Added {filename} to tracker")

print("Done.")
