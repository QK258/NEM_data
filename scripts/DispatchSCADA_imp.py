import os
import zipfile
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from bs4 import BeautifulSoup
from datetime import datetime
import csv

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting SCADA update...")

# --- Paths and URLs ---
BASE_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data"
ZIP_DIR = os.path.join(BASE_DIR, "DispSCADA_zips")
CSV_DIR = os.path.join(BASE_DIR, "DispSCADA_csvs")
PARQUET_DIR = os.path.join(BASE_DIR, "filtered_scada_data")
DB_PATH = os.path.join(BASE_DIR, "scada_tracker.duckdb")
URL = "https://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

# --- Make directories ---
os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

# --- Start DuckDB tracker ---
con = duckdb.connect(DB_PATH)
con.execute("CREATE TABLE IF NOT EXISTS processed_files (filename TEXT PRIMARY KEY)")
existing_files = set(x[0] for x in con.execute("SELECT filename FROM processed_files").fetchall())

# --- Get list of ZIP links ---
print("Scraping SCADA zip links...")
try:
    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    zip_links = [a['href'] for a in soup.find_all("a", href=True) if a['href'].endswith(".zip")]
    print(f"Found {len(zip_links)} zip files")
except Exception as e:
    print(f"Failed to scrape links: {e}")
    exit(1)

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
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"Download failed for {filename}: {e}")
            continue

    # Extract and process ZIP
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                if not member.endswith('.CSV'):
                    continue
                    
                csv_path = os.path.join(CSV_DIR, member)
                if not os.path.exists(csv_path):
                    zf.extract(member, CSV_DIR)

                print(f"Processing {member}")
                
                # Find column names and extract SCADA data
                scada_columns = None
                scada_rows = []
                
                with open(csv_path, 'r', encoding='utf-8') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        if len(row) < 4:
                            continue
                            
                        # Get column names from header row
                        if row[0] == "I" and row[1] == "DISPATCH" and row[2] == "UNIT_SCADA":
                            scada_columns = row[4:]  # Skip D,DISPATCH,UNIT_SCADA,1
                            
                        # Extract SCADA data rows
                        elif row[0] == "D" and row[1] == "DISPATCH" and row[2] == "UNIT_SCADA":
                            scada_data = row[4:]  # Skip D,DISPATCH,UNIT_SCADA,1
                            scada_rows.append(scada_data)

                if not scada_rows or not scada_columns:
                    print(f"No SCADA data found in {member}")
                    continue

                # Create DataFrame
                df = pd.DataFrame(scada_rows, columns=scada_columns)
                df["filename"] = filename
                
                # Parse settlement date
                try:
                    date_col = "SETTLEMENTDATE"  # Standard SCADA column name
                    df[date_col] = df[date_col].str.strip('"')
                    df["dispatch_datetime"] = pd.to_datetime(df[date_col], format="%Y/%m/%d %H:%M:%S")
                    df["dispatch_date"] = df["dispatch_datetime"].dt.date.astype(str)
                    df["dispatch_hour"] = df["dispatch_datetime"].dt.hour
                except Exception as e:
                    print(f"Date parse error in {member}: {e}")
                    continue

                # Convert SCADAVALUE to numeric
                if "SCADAVALUE" in df.columns:
                    df["SCADAVALUE"] = pd.to_numeric(df["SCADAVALUE"], errors='coerce')

                # Clean string columns (remove quotes)
                for col in df.select_dtypes(include=['object']).columns:
                    if col not in ["filename", "dispatch_date"]:
                        df[col] = df[col].astype(str).str.strip('"')

                print(f"Processed {len(df)} SCADA records from {member}")

                # Save as partitioned parquet
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(table, root_path=PARQUET_DIR, partition_cols=["dispatch_date"])

                # Clean up CSV
                os.remove(csv_path)

    except Exception as e:
        print(f"Failed to process {filename}: {e}")
        continue

    # Mark as processed
    con.execute("INSERT OR IGNORE INTO processed_files VALUES (?)", [filename])
    print(f"Added {filename} to tracker")

con.close()
print("SCADA download complete.")