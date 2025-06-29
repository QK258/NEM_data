import os
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from datetime import datetime, timedelta
import re

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting Historical RRP Data Import...")

# ============================================================================
# CONFIGURATION
# ============================================================================
# Historical data source
HISTORIC_DATA_DIR = r"C:\Users\user\Google Drive\Personal\Energy Dashboard\Historic Spot Prices\2022- jun 2024 data"

# Existing database paths
BASE_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data"
PARQUET_DIR = os.path.join(BASE_DIR, "Price_RRP_data")
DB_PATH = os.path.join(BASE_DIR, "Price_RRP_tracker.duckdb")

# Configuration
TARGET_YEAR = 2024              # Year to process (change for other years)
BATCH_SIZE = 10000              # Records to process in batches
CUTOFF_DATE = "2024-06-09"      # Don't import data after this (you already have it)

PRICE_COLUMNS = ["SETTLEMENTDATE", "REGIONID", "PERIODID", "RRP", "LASTCHANGED"]

print(f"📅 Target Year: {TARGET_YEAR}")
print(f"📂 Source Directory: {HISTORIC_DATA_DIR}")
print(f"⚠️  Cutoff Date: {CUTOFF_DATE} (avoiding duplicates)")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def find_csv_files():
    """Find all CSV files for the target year"""
    pattern = os.path.join(HISTORIC_DATA_DIR, "*.csv")
    all_files = glob.glob(pattern)
    
    # Filter files that likely contain the target year
    year_files = []
    for file in all_files:
        filename = os.path.basename(file)
        if str(TARGET_YEAR) in filename:
            year_files.append(file)
    
    print(f"🔍 Found {len(all_files)} total CSV files")
    print(f"📋 Found {len(year_files)} files matching year {TARGET_YEAR}")
    
    if year_files:
        print("   Sample files:")
        for file in year_files[:3]:
            print(f"     - {os.path.basename(file)}")
        if len(year_files) > 3:
            print(f"     ... and {len(year_files) - 3} more files")
    
    return year_files

def parse_settlement_date(date_str):
    """Convert settlement date to datetime using pandas auto-detection"""
    try:
        # Use pandas auto-detection - works perfectly for your YYYY/MM/DD HH:MM:SS format
        return pd.to_datetime(date_str)
    except:
        return None

def calculate_period_id(settlement_date):
    """Calculate PERIODID from settlement date (1-288 for each 5-min interval)"""
    try:
        # Get total minutes since midnight
        hour = settlement_date.hour
        minute = settlement_date.minute
        total_minutes = hour * 60 + minute
        
        # Calculate period: each 5-minute interval is one period
        # Formula: ((total_minutes - 1) // 5) + 1
        # This ensures 00:05 = period 1, 18:35 = period 223, etc.
        if total_minutes == 0:
            return 288  # Midnight (00:00) is the last period of previous day
        else:
            period_id = ((total_minutes - 1) // 5) + 1
        
        return period_id
    except:
        return None

def process_csv_file(file_path):
    """Process a single CSV file and return cleaned data"""
    filename = os.path.basename(file_path)
    print(f"   📄 Processing: {filename}")
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        print(f"     Raw records: {len(df):,}")
        
        # Check if required columns exist
        required_cols = ['REGION', 'SETTLEMENTDATE', 'RRP']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"     ❌ Missing columns: {missing_cols}")
            return pd.DataFrame()
        
        # Filter to only needed columns
        df = df[['REGION', 'SETTLEMENTDATE', 'RRP']].copy()
        
        # Rename REGION to REGIONID for consistency
        df.rename(columns={'REGION': 'REGIONID'}, inplace=True)
        
        # Parse settlement dates using pandas vectorized auto-detection
        df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'], errors='coerce')
        
        # Remove records with invalid dates
        df = df.dropna(subset=['SETTLEMENTDATE'])
        print(f"     Valid dates: {len(df):,}")
        
        # Filter to target year only
        df = df[df['SETTLEMENTDATE'].dt.year == TARGET_YEAR]
        print(f"     Year {TARGET_YEAR}: {len(df):,}")
        
        # Filter out data after cutoff date (to avoid duplicates)
        cutoff_dt = pd.to_datetime(CUTOFF_DATE)
        df = df[df['SETTLEMENTDATE'] < cutoff_dt]
        print(f"     Before cutoff: {len(df):,}")
        
        if df.empty:
            print(f"     ⚠️  No data remaining after filters")
            return df
        
        # Convert RRP to numeric
        df['RRP'] = pd.to_numeric(df['RRP'], errors='coerce')
        df = df.dropna(subset=['RRP'])
        
        # Calculate PERIODID vectorized (much faster than apply)
        total_minutes = df['SETTLEMENTDATE'].dt.hour * 60 + df['SETTLEMENTDATE'].dt.minute
        df['PERIODID'] = ((total_minutes - 1) // 5) + 1
        
        # Handle midnight case (00:00 becomes period 288 of previous day)
        df.loc[total_minutes == 0, 'PERIODID'] = 288
        df['PERIODID'] = df['PERIODID'].astype(int)
        
        # Add LASTCHANGED (use settlement date as placeholder)
        df['LASTCHANGED'] = df['SETTLEMENTDATE']
        
        # Add partitioning columns
        df['year'] = df['SETTLEMENTDATE'].dt.year
        df['month'] = df['SETTLEMENTDATE'].dt.month
        
        # Final data validation
        df = df.dropna(subset=['REGIONID', 'SETTLEMENTDATE', 'RRP'])
        
        print(f"     ✅ Final records: {len(df):,}")
        return df
        
    except Exception as e:
        print(f"     ❌ Error processing {filename}: {e}")
        return pd.DataFrame()

def save_batch_to_parquet(batch_data):
    """Save batch of data to parquet with partitioning"""
    if batch_data.empty:
        return 0
    
    try:
        # Ensure correct data types
        batch_data['REGIONID'] = batch_data['REGIONID'].astype('category')
        batch_data['PERIODID'] = batch_data['PERIODID'].astype('int16')
        batch_data['RRP'] = pd.to_numeric(batch_data['RRP'])
        
        # Sort for better compression
        batch_data = batch_data.sort_values(['SETTLEMENTDATE', 'REGIONID'])
        
        # Write to parquet with partitioning
        table = pa.Table.from_pandas(
            batch_data[PRICE_COLUMNS + ['year', 'month']], 
            preserve_index=False
        )
        
        pq.write_to_dataset(
            table,
            root_path=PARQUET_DIR,
            partition_cols=["year", "month", "REGIONID"],
            existing_data_behavior="overwrite_or_ignore"
        )
        
        return len(batch_data)
        
    except Exception as e:
        print(f"     ❌ Error saving batch: {e}")
        return 0

def get_existing_data_info():
    """Check what data already exists in the database"""
    try:
        con = duckdb.connect(DB_PATH)
        
        # Check date range of existing data
        query = f"""
        SELECT 
            MIN(SETTLEMENTDATE) as min_date,
            MAX(SETTLEMENTDATE) as max_date,
            COUNT(*) as total_records,
            COUNT(DISTINCT REGIONID) as regions
        FROM read_parquet('{PARQUET_DIR.replace(chr(92), '/')}/**/*.parquet')
        WHERE EXTRACT('year' FROM SETTLEMENTDATE) = {TARGET_YEAR}
        """
        
        result = con.execute(query).fetchone()
        con.close()
        
        if result and result[0]:
            print(f"📊 Existing {TARGET_YEAR} data:")
            print(f"   Date range: {result[0]} to {result[1]}")
            print(f"   Records: {result[2]:,}")
            print(f"   Regions: {result[3]}")
            return True
        else:
            print(f"📊 No existing {TARGET_YEAR} data found")
            return False
            
    except Exception as e:
        print(f"⚠️  Could not check existing data: {e}")
        return False

# ============================================================================
# MAIN EXECUTION
# ============================================================================
start_time = datetime.now()

# Check existing data
get_existing_data_info()

# Find CSV files
csv_files = find_csv_files()

if not csv_files:
    print("❌ No CSV files found for processing")
    exit()

# Database setup
try:
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS processed_files_historic (filename TEXT PRIMARY KEY, processed_time TEXT)")
    existing_files = set(x[0] for x in con.execute("SELECT filename FROM processed_files_historic").fetchall())
    con.close()
    
    print(f"📊 Database: {len(existing_files)} historic files already processed")
except Exception as e:
    print(f"❌ Database setup failed: {e}")
    exit()

# Process files
print(f"\n🚀 Processing {len(csv_files)} CSV files...")

total_records = 0
processed_files = []
all_data = []

for i, file_path in enumerate(csv_files):
    filename = os.path.basename(file_path)
    
    # Skip if already processed
    if filename in existing_files:
        print(f"   ⏭️  SKIPPING: {filename} (already processed)")
        continue
    
    print(f"\n📁 FILE {i+1}/{len(csv_files)}")
    
    # Process file
    file_data = process_csv_file(file_path)
    
    if not file_data.empty:
        all_data.append(file_data)
        processed_files.append(filename)
        
        # Process in batches to avoid memory issues
        if len(all_data) >= 5:  # Process every 5 files
            combined_data = pd.concat(all_data, ignore_index=True)
            records_saved = save_batch_to_parquet(combined_data)
            total_records += records_saved
            
            print(f"     💾 Batch saved: {records_saved:,} records")
            all_data = []  # Clear batch

# Process remaining data
if all_data:
    combined_data = pd.concat(all_data, ignore_index=True)
    records_saved = save_batch_to_parquet(combined_data)
    total_records += records_saved
    print(f"     💾 Final batch: {records_saved:,} records")

# Update database tracking
if processed_files:
    try:
        con = duckdb.connect(DB_PATH)
        now = datetime.now().isoformat()
        
        for filename in processed_files:
            con.execute(
                "INSERT OR IGNORE INTO processed_files_historic VALUES (?, ?)",
                (filename, now)
            )
        
        # Update summary table
        if total_records > 0:
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
        
        con.close()
        
    except Exception as e:
        print(f"⚠️  Database update failed: {e}")

# Final results
elapsed_time = datetime.now() - start_time

print(f"\n🎉 HISTORICAL DATA IMPORT COMPLETE!")
print(f"⏱️  Time: {elapsed_time}")
print(f"📊 Files: {len(processed_files)}/{len(csv_files)} processed")
print(f"📈 Records: {total_records:,} imported")
print(f"💾 Data saved to: {PARQUET_DIR}")

if total_records > 0:
    print(f"✅ Historical data successfully integrated!")
    print(f"💡 Now you have {TARGET_YEAR} data from January to June 8th")
    print(f"💡 Plus current data from June 9th onwards")
else:
    print(f"💡 No new data was imported")

# Show final data summary
try:
    con = duckdb.connect(DB_PATH)
    summary_query = f"""
    SELECT 
        EXTRACT('month' FROM SETTLEMENTDATE) as month,
        COUNT(*) as records,
        MIN(SETTLEMENTDATE) as start_date,
        MAX(SETTLEMENTDATE) as end_date
    FROM read_parquet('{PARQUET_DIR.replace(chr(92), '/')}/**/*.parquet')
    WHERE EXTRACT('year' FROM SETTLEMENTDATE) = {TARGET_YEAR}
    GROUP BY EXTRACT('month' FROM SETTLEMENTDATE)
    ORDER BY month
    """
    
    summary = con.execute(summary_query).fetchdf()
    con.close()
    
    if not summary.empty:
        print(f"\n📊 {TARGET_YEAR} Data Summary by Month:")
        print(summary.to_string(index=False))
    
except Exception as e:
    print(f"⚠️  Could not generate summary: {e}")