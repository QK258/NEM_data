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
PARQUET_DIR = os.path.join(BASE_DIR, "monthly_price_data")
DB_PATH = os.path.join(BASE_DIR, "price_tracker.duckdb")
URL = "https://nemweb.com.au/Reports/CURRENT/TradingIS_Reports/"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

# --- Make directories ---
os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

# --- Price columns (manually defined) ---
PRICE_COLUMNS = [
    "SETTLEMENTDATE", "RUNNO", "REGIONID", "PERIODID", "RRP", "EEP", "INVALIDFLAG", "LASTCHANGED", "RAISE6SECRRP", "RAISE60SECRRP", "RAISE5MINRRP",
    "RAISEREGRRP", "LOWER6SECRRP","LOWER60SECRRP","LOWER5MINRRP", "LOWERREGRRP","RAISE1SECRRP","LOWER1SECRRP", "PRICE_STATUS"
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

                df = pd.DataFrame(price_rows, columns=['prefix1', 'prefix2', 'block', 'version'] + PRICE_COLUMNS)
                df = df.drop(columns=['prefix1', 'prefix2', 'block', 'version'])

                df = df.applymap(lambda x: x.strip('"'))

                df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S")
                df["RRP"] = df["RRP"].astype(float)
                df["REGIONID"] = df["REGIONID"].astype("category")
                df["month"] = df["SETTLEMENTDATE"].dt.to_period("M").astype(str)

                df["filename"] = filename
                df = df[sorted(df.columns)]  # ⬅️ ensure consistent column order
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(table, root_path=PARQUET_DIR, partition_cols=["month"])

    except Exception as e:
        print(f"Failed to process {filename}: {e}")
        continue

    con.execute("INSERT OR IGNORE INTO processed_files VALUES (?)", [filename])
    print(f"Added {filename} to tracker")

print("Done.")