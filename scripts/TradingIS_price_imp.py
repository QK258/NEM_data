import os
import zipfile
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import time

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting FAST current price update...")

# ============================================================================
# PATHS & CONFIGURATION
# ============================================================================
BASE_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data"
ZIP_DIR = os.path.join(BASE_DIR, "Price_TradingIS_zips")
PARQUET_DIR = os.path.join(BASE_DIR, "Price_RRP_data")
DB_PATH = os.path.join(BASE_DIR, "Price_RRP_tracker.duckdb")

URL = "https://nemweb.com.au/Reports/Current/TradingIS_Reports/"
BASE_DOMAIN = "https://nemweb.com.au"  # For constructing download URLs
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# Speed optimization settings
MAX_WORKERS = 4          # Reduced for stability
BATCH_SIZE = 100         # Smaller batches for reliability
TIMEOUT = 30             # Longer timeout
DAYS_BACK = 7            # Reduced window for testing

PRICE_COLUMNS = ["SETTLEMENTDATE", "REGIONID", "PERIODID", "RRP", "LASTCHANGED"]

os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

# ============================================================================
# DEBUGGING FUNCTION
# ============================================================================
def debug_website_access():
    """Test website access and structure"""
    print("🔍 DEBUG: Testing website access...")
    
    try:
        response = requests.get(URL, headers=HEADERS, timeout=30)
        print(f"   Status Code: {response.status_code}")
        print(f"   Content Length: {len(response.text)} characters")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            all_links = soup.find_all("a", href=True)
            print(f"   Total Links Found: {len(all_links)}")
            
            # Look for zip files specifically
            zip_links = [a['href'] for a in all_links if a.get('href', '').endswith(".zip")]
            print(f"   ZIP Files Found: {len(zip_links)}")
            
            if zip_links:
                print(f"   Sample ZIP files: {zip_links[0]}, {zip_links[1] if len(zip_links) > 1 else 'N/A'}, ...")
            
            return zip_links
        else:
            print(f"   ❌ Failed to access website: HTTP {response.status_code}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Network error: {e}")
        return []
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        return []

# ============================================================================
# FAST DOWNLOAD FUNCTION (IMPROVED)
# ============================================================================
def download_file_fast(url_filename_tuple):
    """Download single file with better error handling and proper URL construction"""
    link, filename = url_filename_tuple
    zip_path = os.path.join(ZIP_DIR, filename)
    
    # Skip if already exists and is not empty
    if os.path.exists(zip_path) and os.path.getsize(zip_path) > 0:
        return filename, "exists"
    
    try:
        # Construct proper URL - handle both absolute and relative links
        if link.startswith('http'):
            full_url = link
        elif link.startswith('/'):
            # Relative link starting with / - use base domain
            full_url = BASE_DOMAIN + link
        else:
            # Relative link without / - use full base URL
            full_url = URL + link
            
        response = requests.get(full_url, headers=HEADERS, timeout=TIMEOUT, stream=True)
        response.raise_for_status()
        
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Verify file was downloaded and has content
        if os.path.getsize(zip_path) > 0:
            return filename, "downloaded"
        else:
            os.remove(zip_path)  # Remove empty file
            return filename, "failed"
            
    except requests.exceptions.RequestException as e:
        return filename, "failed"
    except Exception as e:
        return filename, "failed"

# ============================================================================
# IMPROVED DATE FILTERING
# ============================================================================
def filter_recent_files(all_links, days_back=DAYS_BACK):
    """Filter files by date with better pattern matching"""
    cutoff_date = datetime.now() - timedelta(days=days_back)
    recent_links = []
    
    print(f"🗓️  Looking for files newer than: {cutoff_date.strftime('%Y-%m-%d')}")
    
    for link in all_links:
        # Try multiple date patterns
        date_patterns = [
            r'(\d{8})',           # YYYYMMDD
            r'(\d{4})(\d{2})(\d{2})',  # YYYY MM DD
            r'(\d{2})(\d{2})(\d{4})',  # DD MM YYYY
        ]
        
        file_date = None
        for pattern in date_patterns:
            date_match = re.search(pattern, link)
            if date_match:
                try:
                    if len(date_match.groups()) == 1:
                        # YYYYMMDD format
                        file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
                    elif len(date_match.groups()) == 3:
                        # Try different combinations
                        groups = date_match.groups()
                        if len(groups[0]) == 4:  # YYYY MM DD
                            file_date = datetime.strptime(f"{groups[0]}{groups[1]}{groups[2]}", '%Y%m%d')
                        else:  # DD MM YYYY
                            file_date = datetime.strptime(f"{groups[2]}{groups[1]}{groups[0]}", '%Y%m%d')
                    
                    if file_date and file_date >= cutoff_date:
                        recent_links.append(link)
                    break
                except ValueError:
                    continue
    
    if not recent_links:
        print(f"   ❌ No files found in the last {days_back} days")
        print("   💡 Try increasing DAYS_BACK or check if files exist on the website")
    else:
        print(f"   ✅ Found {len(recent_links)} recent files")
    
    return recent_links

# ============================================================================
# FAST CSV PROCESSING FUNCTION (UNCHANGED)
# ============================================================================
def process_file_fast(filename):
    """Process single file in memory - no disk extraction"""
    zip_path = os.path.join(ZIP_DIR, filename)
    
    if not os.path.exists(zip_path):
        return []
    
    all_data = []
    
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for csv_name in zf.namelist():
                if not csv_name.lower().endswith('.csv'):
                    continue
                
                # Process CSV directly from memory - NO DISK EXTRACTION
                with zf.open(csv_name) as csv_file:
                    content = csv_file.read().decode('utf-8')
                    reader = csv.reader(content.splitlines())
                    
                    price_header = None
                    
                    for row in reader:
                        if len(row) >= 3:
                            if row[:3] == ['I', 'TRADING', 'PRICE']:
                                price_header = PRICE_COLUMNS
                            elif row[:3] == ['D', 'TRADING', 'PRICE'] and price_header:
                                if len(row) >= 12:
                                    try:
                                        all_data.append({
                                            "SETTLEMENTDATE": row[4].replace('"', '').strip(),
                                            "REGIONID": row[6].replace('"', '').strip(),
                                            "PERIODID": row[7].replace('"', '').strip(),
                                            "RRP": row[8].replace('"', '').strip(),
                                            "LASTCHANGED": row[11].replace('"', '').strip(),
                                            "filename": filename
                                        })
                                    except:
                                        continue
        return all_data
    except Exception as e:
        print(f"   ❌ Error processing {filename}: {e}")
        return []

# ============================================================================
# BATCH PARQUET WRITER (UNCHANGED)
# ============================================================================
def write_batch_to_parquet(batch_data):
    """Write batch of data to parquet efficiently"""
    if not batch_data:
        return 0
    
    try:
        df = pd.DataFrame(batch_data)
        
        # Fast data type conversion
        df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'], errors='coerce')
        df = df.dropna(subset=['SETTLEMENTDATE'])
        
        if df.empty:
            return 0
        
        # Add partitioning columns
        df['year'] = df['SETTLEMENTDATE'].dt.year
        df['month'] = df['SETTLEMENTDATE'].dt.month
        
        # Optimize data types
        df["RRP"] = pd.to_numeric(df["RRP"], errors='coerce')
        df["REGIONID"] = df["REGIONID"].astype("category")
        df["PERIODID"] = pd.to_numeric(df["PERIODID"], errors='coerce').astype("int16")
        
        # Remove invalid data
        df = df.dropna(subset=['RRP', 'PERIODID'])
        
        if df.empty:
            return 0
        
        # Sort for compression
        df = df.sort_values(['SETTLEMENTDATE', 'REGIONID'])
        
        # Write to parquet
        table = pa.Table.from_pandas(df[PRICE_COLUMNS + ['year', 'month']], preserve_index=False)
        pq.write_to_dataset(
            table, 
            root_path=PARQUET_DIR, 
            partition_cols=["year", "month", "REGIONID"],
            existing_data_behavior="overwrite_or_ignore"
        )
        
        return len(df)
    except Exception as e:
        print(f"   ❌ Error writing to parquet: {e}")
        return 0

# ============================================================================
# MAIN EXECUTION - IMPROVED WITH DEBUGGING
# ============================================================================
start_time = time.time()

# Database setup
try:
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS processed_files_current (filename TEXT PRIMARY KEY, processed_time TEXT)")
    existing_files = set(x[0] for x in con.execute("SELECT filename FROM processed_files_current").fetchall())
    print(f"📊 Database connected. Previously processed files: {len(existing_files)}")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    exit(1)

try:
    # ========================================================================
    # PHASE 0: DEBUG WEBSITE ACCESS
    # ========================================================================
    all_links = debug_website_access()
    
    if not all_links:
        print("❌ No ZIP files found on website. Exiting.")
        con.close()
        exit(1)
    
    # ========================================================================
    # PHASE 1: FILTER FILES BY DATE
    # ========================================================================
    print(f"\n🚀 Phase 1: Filtering {len(all_links)} files by date...")
    recent_links = filter_recent_files(all_links, DAYS_BACK)
    
    if not recent_links:
        print("💡 Try increasing DAYS_BACK or clearing the database to reprocess files")
        con.close()
        exit(0)
    
    # Filter new files (not in database)
    new_files = []
    for link in recent_links:
        filename = link.split("/")[-1] if "/" in link else link
        if filename not in existing_files:
            new_files.append(link)
    
    # Show summary without listing each file
    if len(recent_links) != len(new_files):
        print(f"   ⏭️  Skipping {len(recent_links) - len(new_files)} already processed files")
    
    print(f"📦 Found {len(new_files)} new files to process")
    
    if not new_files:
        print("💡 No new files to process. All recent files already in database.")
        con.close()
        exit(0)
    
    # ========================================================================
    # PHASE 2: DOWNLOAD FILES
    # ========================================================================
    print(f"\n⚡ Phase 2: Downloading {len(new_files)} files...")
    
    # Prepare download tasks
    download_tasks = []
    for link in new_files:
        filename = link.split("/")[-1] if "/" in link else link
        download_tasks.append((link, filename))
    
    downloaded_files = []
    failed_downloads = 0
    
    # Use sequential downloads with batch progress
    total_tasks = len(download_tasks)
    for i, (link, filename) in enumerate(download_tasks):
        result_filename, status = download_file_fast((link, filename))
        if status in ["downloaded", "exists"]:
            downloaded_files.append(result_filename)
        else:
            failed_downloads += 1
        
        # Show progress every 50 files or at the end
        if (i + 1) % 50 == 0 or (i + 1) == total_tasks:
            percent = ((i + 1) / total_tasks) * 100
            print(f"   Progress: {i + 1}/{total_tasks} ({percent:.1f}%) - Success: {len(downloaded_files)}, Failed: {failed_downloads}")
    
    print(f"✅ Download complete: {len(downloaded_files)} files ready for processing")
    
    if not downloaded_files:
        print("❌ No files were successfully downloaded")
        con.close()
        exit(1)
    
    # ========================================================================
    # PHASE 3: PROCESS FILES
    # ========================================================================
    print(f"\n⚡ Phase 3: Processing {len(downloaded_files)} files...")
    
    total_records = 0
    processed_files = []
    
    for i, filename in enumerate(downloaded_files):
        print(f"   Processing {i+1}/{len(downloaded_files)}: {filename}")
        file_data = process_file_fast(filename)
        
        if file_data:
            records_written = write_batch_to_parquet(file_data)
            total_records += records_written
            processed_files.append(filename)
            print(f"     ✅ {records_written} records written")
        else:
            print(f"     ⚠️  No valid data found")
    
    # ========================================================================
    # PHASE 4: UPDATE DATABASE
    # ========================================================================
    print(f"\n⚡ Phase 4: Updating database...")
    
    if processed_files:
        now = datetime.now().isoformat()
        con.executemany(
            "INSERT OR IGNORE INTO processed_files_current VALUES (?, ?)",
            [(f, now) for f in processed_files]
        )
        print(f"   ✅ Added {len(processed_files)} files to database")
    
    # Update summary table
    if total_records > 0:
        try:
            con.execute(f"""
            CREATE OR REPLACE TABLE price_summary AS
            SELECT 
                REGIONID, year, month,
                MIN(RRP) as min_price, MAX(RRP) as max_price, AVG(RRP) as avg_price,
                COUNT(*) as record_count
            FROM read_parquet('{PARQUET_DIR.replace(chr(92), '/')}/**/*.parquet')
            WHERE RRP IS NOT NULL
            GROUP BY REGIONID, year, month
            ORDER BY REGIONID, year, month
            """)
            print(f"   ✅ Summary table updated")
        except Exception as e:
            print(f"   ⚠️  Summary table update failed: {e}")

finally:
    con.close()

# ============================================================================
# FINAL RESULTS
# ============================================================================
elapsed_time = time.time() - start_time
files_per_second = len(new_files) / elapsed_time if elapsed_time > 0 and new_files else 0

print(f"\n🎉 COMPLETE!")
print(f"⏱️  Time: {elapsed_time:.1f} seconds ({files_per_second:.1f} files/sec)")
print(f"📊 Files: {len(processed_files)}/{len(new_files)} processed successfully")
print(f"📈 Records: {total_records:,} written to database")

if total_records > 0:
    print(f"✅ Database updated and ready for analysis!")
else:
    print(f"💡 No new data processed. Check the debug output above for issues.")