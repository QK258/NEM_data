"""
================================================================================
BATTERY ARBITRAGE REVENUE ANALYSIS 
================================================================================

PURPOSE:
This script analyzes battery energy storage revenue potential in the Australian 
National Electricity Market (NEM) using 5-minute Regional Reference Price (RRP) data.

STRATEGY:
- Each day, find the best 2-hour window to charge the battery (lowest prices)
- Each day, find the best 2-hour window to discharge the battery (highest prices)  
- Calculate daily profit from this "buy low, sell high" arbitrage strategy
- Export results to CSV for further analysis

KEY ASSUMPTIONS:
- Battery can do exactly one charge-discharge cycle per day
- Charging and discharging windows can overlap (simplified model)
- Battery efficiency losses occur during the full charge-discharge cycle
- Prices are known in advance (perfect foresight - not realistic but useful for potential analysis)
- Battery can charge/discharge at constant rate over 2-hour windows
- No degradation, maintenance costs, or other operational expenses included

DATA STRUCTURE:
- NEM 5-minute RRP data: 288 periods per day (24 hours × 12 periods/hour)
- 2-hour windows: 24 consecutive 5-minute periods
- Region-specific pricing (VIC1, NSW1, QLD1, SA1, TAS1)

FINANCIAL CALCULATIONS:
- Charging Cost = Average Charge Price × Battery Capacity
- Discharge Revenue = Average Discharge Price × Battery Capacity × Efficiency  
- Daily Profit = Discharge Revenue - Charging Cost
================================================================================
"""

import duckdb
import pandas as pd
import os

print("🔋 BATTERY ANALYSIS - MAY 2025 DAILY WINDOWS")
print("=" * 60)

# ============================================================================
# CONFIGURATION PARAMETERS
# ============================================================================
REGION = 'VIC1'        # Australian NEM region (VIC1, NSW1, QLD1, SA1, TAS1)
BATTERY_MWH = 10       # Battery energy capacity in MegaWatt hours
EFFICIENCY = 0.85      # Round-trip efficiency: energy out / energy in
                       # Typical range: 0.80-0.95 for modern batteries

# ============================================================================
# DATABASE CONNECTION SETUP
# ============================================================================
DB_PATH = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data\Price_RRP_tracker.duckdb"
PARQUET_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data\Price_RRP_data"

# Report directory setup
REPORT_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\reports"
os.makedirs(REPORT_DIR, exist_ok=True)  # Create reports folder if it doesn't exist

# Connect to DuckDB database containing our price data
con = duckdb.connect(DB_PATH)

print(f"📊 Analyzing {REGION} for June 2024")
print(f"🔋 Battery: {BATTERY_MWH} MWh, Efficiency: {EFFICIENCY*100}%")
print("=" * 60)

# ============================================================================
# STEP 1: BUILD SQL QUERY TO FIND OPTIMAL DAILY CHARGING/DISCHARGING WINDOWS
# ============================================================================
print("1️⃣ Calculating 2-hour rolling windows for each day...")

# This complex SQL query uses Common Table Expressions (CTEs) to:
# 1. Get all 5-minute price data for May 2025
# 2. Calculate 2-hour rolling average prices
# 3. Find the best charging window (lowest price) per day
# 4. Find the best discharging window (highest price) per day

query = f"""
WITH daily_data AS (
    -- ===================================================================
    -- CTE 1: Extract and organize all 5-minute intervals for May 2025
    -- ===================================================================
    -- Purpose: Get raw data and add day grouping + sequential numbering
    -- Result: Each 5-min interval gets a period number within each day
    
    SELECT 
        SETTLEMENTDATE,                          -- Exact timestamp (e.g., '2025-05-01 14:05:00')
        DATE_TRUNC('day', SETTLEMENTDATE) as trade_date,  -- Just the date part (e.g., '2025-05-01')
        RRP,                                     -- Regional Reference Price in $/MWh
        PERIODID,                               -- NEM period ID (1-288 for each 5-min interval)
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', SETTLEMENTDATE) 
            ORDER BY SETTLEMENTDATE
        ) as period_of_day                      -- Sequential number 1-288 within each day
    FROM read_parquet('{PARQUET_DIR}/year=2024/month=6/REGIONID={REGION}/*.parquet') 
    ORDER BY SETTLEMENTDATE
),

rolling_windows AS (
    -- ===================================================================
    -- CTE 2: Calculate 2-hour rolling window averages
    -- ===================================================================
    -- Purpose: For each 5-min period, calculate the average price of the 
    --         next 2 hours (24 periods × 5 minutes = 120 minutes = 2 hours)
    -- Strategy: A battery could start charging/discharging at this time
    --          and continue for exactly 2 hours
    
    SELECT 
        trade_date,
        period_of_day,
        SETTLEMENTDATE as window_start,          -- When this 2-hour window starts
        
        -- Calculate average RRP for this 2-hour window (current + next 23 periods)
        AVG(RRP) OVER (
            PARTITION BY trade_date              -- Calculate separately for each day
            ORDER BY period_of_day 
            ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING  -- Current + next 23 = 24 total periods
        ) as window_avg_price,
        
        -- Calculate when this 2-hour window ends (start + 2 hours)
        window_start + INTERVAL '2 hours' as window_end,
        
        -- Count how many periods are actually in this window (should be 24)
        COUNT(*) OVER (
            PARTITION BY trade_date 
            ORDER BY period_of_day 
            ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING
        ) as periods_in_window
        
    FROM daily_data
),

complete_windows AS (
    -- ===================================================================
    -- CTE 3: Filter to only complete 2-hour windows
    -- ===================================================================
    -- Purpose: Remove partial windows at the end of each day
    -- Result: Only windows with exactly 24 periods (full 2 hours)
    
    SELECT * 
    FROM rolling_windows 
    WHERE periods_in_window = 24        -- Must have complete 2-hour window
),

ranked_windows AS (
    -- ===================================================================
    -- CTE 4A: Rank windows by price for each day
    -- ===================================================================
    SELECT 
        trade_date,
        window_start,
        window_avg_price,
        period_of_day,
        -- Rank by lowest price (for charging)
        ROW_NUMBER() OVER (
            PARTITION BY trade_date 
            ORDER BY window_avg_price ASC, period_of_day ASC
        ) as charge_rank,
        -- Rank by highest price (for discharging)
        ROW_NUMBER() OVER (
            PARTITION BY trade_date 
            ORDER BY window_avg_price DESC, period_of_day ASC
        ) as discharge_rank
    FROM complete_windows
),

daily_optimal AS (
    -- ===================================================================
    -- CTE 4B: Get the best charging and discharging windows
    -- ===================================================================
    SELECT 
        trade_date,
        
        -- Best charging window (rank 1 for lowest price)
        MAX(CASE WHEN charge_rank = 1 THEN window_start END) as charge_start_time,
        MAX(CASE WHEN charge_rank = 1 THEN window_start + INTERVAL '2 hours' END) as charge_end_time,
        MAX(CASE WHEN charge_rank = 1 THEN window_avg_price END) as charge_avg_price,
        
        -- Best discharging window (rank 1 for highest price)
        MAX(CASE WHEN discharge_rank = 1 THEN window_start END) as discharge_start_time,
        MAX(CASE WHEN discharge_rank = 1 THEN window_start + INTERVAL '2 hours' END) as discharge_end_time,
        MAX(CASE WHEN discharge_rank = 1 THEN window_avg_price END) as discharge_avg_price
        
    FROM ranked_windows
    WHERE charge_rank = 1 OR discharge_rank = 1
    GROUP BY trade_date
)

-- ===================================================================
-- FINAL SELECT: Get one row per day with optimal windows
-- ===================================================================
SELECT 
    trade_date,
    charge_start_time,
    charge_end_time,
    ROUND(charge_avg_price, 2) as charge_avg_price,      -- Round to cents
    discharge_start_time,
    discharge_end_time,
    ROUND(discharge_avg_price, 2) as discharge_avg_price  -- Round to cents
FROM daily_optimal
ORDER BY trade_date
"""

# ============================================================================
# STEP 2: EXECUTE QUERY AND HANDLE RESULTS
# ============================================================================
print("🔍 Running query...")
try:
    # Execute the complex SQL query and convert results to pandas DataFrame
    results = con.execute(query).fetchdf()
    print(f"✅ Found data for {len(results)} days in May 2025")
except Exception as e:
    print(f"❌ Error: {e}")
    print("💡 Check if you have May 2025 data in your parquet files")
    exit()

# ============================================================================
# STEP 3: CALCULATE FINANCIAL METRICS
# ============================================================================
print("\n2️⃣ Calculating daily costs, revenues, and profits...")

# CHARGING COST CALCULATION
# Formula: Charge Price ($/MWh) × Battery Capacity (MWh) = Total Cost ($)
# Example: $50/MWh × 10 MWh = $500 to fully charge
results['charging_cost_total'] = results['charge_avg_price'] * BATTERY_MWH

# DISCHARGING REVENUE CALCULATION  
# Formula: Discharge Price ($/MWh) × Battery Capacity (MWh) × Efficiency = Total Revenue ($)
# Example: $100/MWh × 10 MWh × 0.85 = $850 revenue (85% efficiency accounts for energy losses)
results['discharging_revenue_total'] = results['discharge_avg_price'] * BATTERY_MWH * EFFICIENCY

# DAILY PROFIT CALCULATION
# Formula: Revenue - Cost = Profit
# This is the net profit from one charge-discharge cycle per day
results['daily_profit'] = results['discharging_revenue_total'] - results['charging_cost_total']

# PRICE SPREAD CALCULATION
# Formula: Discharge Price - Charge Price = Spread
# This shows the price difference in $/MWh before considering efficiency losses
results['price_spread'] = results['discharge_avg_price'] - results['charge_avg_price']

# Round all financial columns to 2 decimal places (cents)
financial_cols = ['charging_cost_total', 'discharging_revenue_total', 'daily_profit', 'price_spread']
for col in financial_cols:
    results[col] = results[col].round(2)

# ============================================================================
# STEP 4: DISPLAY SUMMARY STATISTICS
# ============================================================================
print("\n3️⃣ Summary Statistics:")
print("-" * 40)

# Basic statistics about the analysis period
print(f"📅 Days analyzed: {len(results)}")

# Financial performance metrics
print(f"💰 Total profit: ${results['daily_profit'].sum():,.2f}")          # Sum all daily profits
print(f"📊 Average daily profit: ${results['daily_profit'].mean():.2f}")   # Mean daily profit
print(f"📈 Best day profit: ${results['daily_profit'].max():.2f}")         # Maximum single day profit
print(f"📉 Worst day profit: ${results['daily_profit'].min():.2f}")        # Minimum single day profit (could be negative)

# Count how many days were profitable (positive profit)
profitable_days = len(results[results['daily_profit'] > 0])
print(f"🎯 Profitable days: {profitable_days}/{len(results)}")

# ============================================================================
# STEP 5: SHOW SAMPLE RESULTS
# ============================================================================
print(f"\n4️⃣ Sample Results (first 5 days):")
print("-" * 40)

# Select key columns to display in a compact format
sample_cols = ['trade_date', 'charge_start_time', 'charge_avg_price', 
               'discharge_start_time', 'discharge_avg_price', 'daily_profit']
print(results[sample_cols].head().to_string(index=False))

# ============================================================================
# STEP 6: PREPARE AND EXPORT CSV FILE
# ============================================================================
print(f"\n5️⃣ Exporting to CSV...")

# Create a clean DataFrame with all relevant columns for CSV export
csv_data = results[[
    'trade_date',                    # Trading date
    'charge_start_time',             # When to start charging  
    'charge_end_time',               # When to stop charging
    'charge_avg_price',              # Average price during charging window
    'discharge_start_time',          # When to start discharging
    'discharge_end_time',            # When to stop discharging 
    'discharge_avg_price',           # Average price during discharging window
    'price_spread',                  # Price difference (before efficiency)
    'charging_cost_total',           # Total cost to charge battery
    'discharging_revenue_total',     # Total revenue from discharging
    'daily_profit'                   # Net profit for the day
]].copy()

# Rename columns to be more descriptive and user-friendly for CSV
csv_data.columns = [
    'Date',
    'Charge_Window_Start',
    'Charge_Window_End', 
    'Charge_Price_$/MWh',
    'Discharge_Window_Start',
    'Discharge_Window_End',
    'Discharge_Price_$/MWh',
    'Price_Spread_$/MWh',
    'Total_Charging_Cost_$',
    'Total_Discharge_Revenue_$',
    'Daily_Profit_$'
]

# Export DataFrame to CSV file in reports folder
csv_filename = f"battery_analysis_{REGION}_May2025.csv"
csv_filepath = os.path.join(REPORT_DIR, csv_filename)
csv_data.to_csv(csv_filepath, index=False)

print(f"✅ Exported to: {csv_filepath}")
print(f"📊 {len(csv_data)} days of data exported")

# ============================================================================
# STEP 7: DETAILED EXAMPLE OF BEST PERFORMING DAY
# ============================================================================
print(f"\n6️⃣ Detailed Example - Best Profit Day:")
print("-" * 50)

# Find the day with maximum profit for detailed breakdown
best_day = results.loc[results['daily_profit'].idxmax()]

# Display detailed breakdown of the most profitable day
print(f"📅 Date: {best_day['trade_date'].strftime('%Y-%m-%d')}")
print(f"")

# CHARGING DETAILS
print(f"⚡ Charge Window: {best_day['charge_start_time'].strftime('%H:%M')} - {best_day['charge_end_time'].strftime('%H:%M')}")
print(f"💰 Charge Price: ${best_day['charge_avg_price']:.2f}/MWh")
print(f"🔋 Charging Cost: ${best_day['charging_cost_total']:.2f} ({BATTERY_MWH} MWh × ${best_day['charge_avg_price']:.2f})")
print(f"")

# DISCHARGING DETAILS  
print(f"⚡ Discharge Window: {best_day['discharge_start_time'].strftime('%H:%M')} - {best_day['discharge_end_time'].strftime('%H:%M')}")
print(f"💰 Discharge Price: ${best_day['discharge_avg_price']:.2f}/MWh")
print(f"🏦 Discharge Revenue: ${best_day['discharging_revenue_total']:.2f} ({BATTERY_MWH} MWh × {EFFICIENCY} × ${best_day['discharge_avg_price']:.2f})")
print(f"")

# PROFIT SUMMARY
print(f"🎯 Daily Profit: ${best_day['daily_profit']:.2f}")
print(f"📊 Price Spread: ${best_day['price_spread']:.2f}/MWh")

print(f"\n🎉 Analysis complete! Check {csv_filepath} for full results.")

# ============================================================================
# STEP 8: ATTEMPT TO OPEN CSV AUTOMATICALLY (OPTIONAL)
# ============================================================================
# Try to open the CSV file automatically in the default application (Excel/etc.)
# This is a convenience feature - if it fails, it won't break the script
try:
    import subprocess
    subprocess.run(['start', csv_filepath], shell=True, check=True)
    print(f"📂 Opened {csv_filename} automatically")
except:
    # If auto-open fails, just continue silently
    pass