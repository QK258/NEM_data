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
import re

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting MTPASA DUID Availability download...")

# --- Paths and URLs ---
BASE_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data"
ZIP_DIR = os.path.join(BASE_DIR, "MTPASA_DUID_Avail_zips")
CSV_DIR = os.path.join(BASE_DIR, "MTPASA_DUID_Avail_csvs")
PARQUET_DIR = os.path.join(BASE_DIR, "MTPASA_DUID_Avail_data")
DB_PATH = os.path.join(BASE_DIR, "MTPASA_DUID_Avail_tracker.duckdb")
URL = "https://nemweb.com.au/Reports/Current/MTPASA_DUIDAvailability/"
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
print("Scraping MTPASA DUID availability zip links...")
try:
    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    zip_links = [a['href'] for a in soup.find_all("a", href=True) if a['href'].endswith(".zip")]
    print(f"Found {len(zip_links)} zip files")
    
    # Filter for 2025 onwards - more precise pattern
    filtered_links = []
    for link in zip_links:
        # Look for 2025, 2026, 2027, etc. in filename
        # Pattern matches: 2025, 2026, 2027, 2028, 2029, 203X, 204X, etc.
        if re.search(r'_202[5-9]|_20[3-9][0-9]', link):
            filtered_links.append(link)
    
    print(f"Found {len(filtered_links)} files from 2025 onwards")
    zip_links = filtered_links
    
except Exception as e:
    print(f"Failed to scrape links: {e}")
    exit(1)

if not zip_links:
    print("No 2025+ files found. Downloading most recent files...")
    # Fallback: get all files and sort by name (usually contains date)
    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    all_links = [a['href'] for a in soup.find_all("a", href=True) if a['href'].endswith(".zip")]
    # Take the most recent files
    zip_links = sorted(all_links)[-10:]  # Last 10 files

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
                
                # Find column names and extract MTPASA DUID data
                mtpasa_columns = None
                mtpasa_rows = []
                
                with open(csv_path, 'r', encoding='utf-8') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        if len(row) < 4:
                            continue
                            
                        # Get column names from header row for MTPASA DUIDAVAILABILITY
                        if (row[0] == "I" and "MTPASA" in row[1] and 
                            ("DUIDAVAILABILITY" in row[2] or "DUIDSUMMARY" in row[2])):
                            mtpasa_columns = row[4:]  # Skip I,MTPASA,DUIDAVAILABILITY,version
                            print(f"  Found columns: {mtpasa_columns}")
                            
                        # Extract MTPASA DUID data rows
                        elif (row[0] == "D" and "MTPASA" in row[1] and 
                              ("DUIDAVAILABILITY" in row[2] or "DUIDSUMMARY" in row[2])):
                            mtpasa_data = row[4:]  # Skip D,MTPASA,DUIDAVAILABILITY,version
                            mtpasa_rows.append(mtpasa_data)

                if not mtpasa_rows or not mtpasa_columns:
                    print(f"No MTPASA DUID data found in {member}")
                    continue

                # Create DataFrame
                df = pd.DataFrame(mtpasa_rows, columns=mtpasa_columns)
                df["filename"] = filename
                
                # Parse date columns if available
                date_columns = [col for col in df.columns if 'DATE' in col.upper() or 'TIME' in col.upper()]
                
                if date_columns:
                    try:
                        main_date_col = date_columns[0]  # Use first date column
                        df[main_date_col] = df[main_date_col].str.strip('"')
                        
                        # Try different date formats
                        for date_format in ["%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"]:
                            try:
                                df["report_datetime"] = pd.to_datetime(df[main_date_col], format=date_format)
                                break
                            except:
                                continue
                        else:
                            # Fallback to pandas auto-parsing
                            df["report_datetime"] = pd.to_datetime(df[main_date_col], errors='coerce')
                        
                        df["report_date"] = df["report_datetime"].dt.date.astype(str)
                        
                    except Exception as e:
                        print(f"Date parse error in {member}: {e}")
                        df["report_date"] = "unknown"
                else:
                    df["report_date"] = "unknown"

                # Convert numeric columns (including additional useful columns)
                numeric_candidates = ["PASAAVAILABILITY", "MAXAVAIL", "CAPACITY", "PASARECALLTIME"]
                for col in df.columns:
                    if any(nc in col.upper() for nc in numeric_candidates):
                        try:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        except:
                            pass

                # Parse additional datetime columns if present
                datetime_candidates = ["LATEST_OFFER_DATETIME", "LASTCHANGED"]
                for col in df.columns:
                    if any(dc in col.upper() for dc in datetime_candidates):
                        try:
                            df[col] = df[col].str.strip('"')
                            # Try to parse as datetime
                            df[col + "_parsed"] = pd.to_datetime(df[col], errors='coerce')
                        except:
                            pass

                # Clean string columns (remove quotes)
                for col in df.select_dtypes(include=['object']).columns:
                    if col not in ["filename", "report_date"]:
                        try:
                            df[col] = df[col].astype(str).str.strip('"')
                        except:
                            pass

                print(f"Processed {len(df)} MTPASA records from {member}")
                print(f"  Columns: {list(df.columns)}")
                
                # Show sample of key columns (including new columns)
                key_cols = [col for col in df.columns if any(key in col.upper() 
                           for key in ['DUID', 'REGION', 'PASAAVAIL', 'CAPACITY', 'PASAUNITSTATE', 
                                     'PASARECALLTIME', 'LATEST_OFFER', 'LASTCHANGED'])]
                if key_cols:
                    print(f"  Sample data: {df[key_cols].head(3).to_dict('records')}")

                # Save as partitioned parquet
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(table, root_path=PARQUET_DIR, partition_cols=["report_date"])

                # Clean up CSV
                os.remove(csv_path)

    except Exception as e:
        print(f"Failed to process {filename}: {e}")
        continue

    # Mark as processed
    con.execute("INSERT OR IGNORE INTO processed_files VALUES (?)", [filename])
    print(f"Added {filename} to tracker")

con.close()

print(f"\nMTPASA DUID Availability download complete!")
print(f"Data saved to: {PARQUET_DIR}")
print(f"To read the data:")
print(f"import pandas as pd")
print(f"df = pd.read_parquet('{PARQUET_DIR}')")
print(f"print(df.columns)")
print(f"print(df[['DUID', 'REGIONID', 'PASAAVAILABILITY', 'PASAUNITSTATE']].head())")