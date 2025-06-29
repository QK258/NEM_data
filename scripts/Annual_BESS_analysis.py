"""
================================================================================
ANNUAL BATTERY ARBITRAGE REVENUE ANALYSIS - 2024 FULL YEAR
================================================================================

PURPOSE:
Comprehensive annual analysis of battery energy storage revenue potential across
all Australian NEM regions using full year of 5-minute RRP data.

ENHANCED FEATURES:
- Annual analysis with monthly/quarterly/seasonal breakdowns
- Multi-region comparison (all 5 NEM regions)
- Seasonal trend analysis and volatility metrics
- Advanced financial metrics and performance indicators
- Detailed reporting with multiple CSV exports
- Statistical analysis of price patterns and arbitrage opportunities

ANALYSIS SCOPE:
- Period: Full calendar year (2024)
- Regions: NSW1, VIC1, QLD1, SA1, TAS1
- Strategy: Daily optimal 2-hour charge/discharge windows
- Output: Multiple reports for different time aggregations

KEY METRICS:
- Daily, monthly, quarterly, and annual profitability
- Seasonal volatility and price spread analysis
- Regional performance comparison
- Success rate and risk metrics
- Efficiency impact analysis
================================================================================
"""

import duckdb
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("🔋 ANNUAL BATTERY ARBITRAGE ANALYSIS - 2024")
print("=" * 70)

# ============================================================================
# ENHANCED CONFIGURATION PARAMETERS
# ============================================================================
ANALYSIS_YEAR = 2024                    # Year to analyze
REGIONS = ['NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1']  # All NEM regions
BATTERY_MWH = 10                        # Battery capacity in MWh
EFFICIENCY = 0.85                       # Round-trip efficiency (85%)

# Battery cost assumptions for ROI analysis
BATTERY_CAPEX = 1500000                 # Capital cost: $150/kWh × 10,000 kWh = $1.5M
ANNUAL_OPEX = 50000                     # Annual operational costs: $50k
BATTERY_LIFE_YEARS = 15                 # Expected operational life

# Analysis parameters
CHARGE_DISCHARGE_HOURS = 2              # Window length for charge/discharge
MIN_SPREAD_THRESHOLD = 10               # Minimum $/MWh spread to consider profitable

print(f"📅 Analysis Year: {ANALYSIS_YEAR}")
print(f"🌏 Regions: {', '.join(REGIONS)}")
print(f"🔋 Battery: {BATTERY_MWH} MWh, {EFFICIENCY*100}% efficiency")
print(f"💰 CAPEX: ${BATTERY_CAPEX:,}, OPEX: ${ANNUAL_OPEX:,}/year")
print("=" * 70)

# ============================================================================
# DATABASE AND FILE SETUP
# ============================================================================
DB_PATH = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data\Price_RRP_tracker.duckdb"
PARQUET_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data\Price_RRP_data"
REPORT_DIR = r"C:\Users\user\Google Drive\Projects\Electricity Prices\reports\annual_analysis"

# Create enhanced reporting directory structure
os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(os.path.join(REPORT_DIR, "regional"), exist_ok=True)
os.makedirs(os.path.join(REPORT_DIR, "monthly"), exist_ok=True)

con = duckdb.connect(DB_PATH)

# ============================================================================
# ENHANCED SQL QUERY FOR ANNUAL ANALYSIS
# ============================================================================
def build_annual_query(region, year):
    """Build optimized SQL query for annual battery arbitrage analysis"""
    
    return f"""
    WITH daily_data AS (
        -- Get all 5-minute intervals for the full year
        SELECT 
            SETTLEMENTDATE,
            DATE_TRUNC('day', SETTLEMENTDATE) as trade_date,
            EXTRACT('month' FROM SETTLEMENTDATE) as month,
            EXTRACT('quarter' FROM SETTLEMENTDATE) as quarter,
            CASE 
                WHEN EXTRACT('month' FROM SETTLEMENTDATE) IN (12, 1, 2) THEN 'Summer'
                WHEN EXTRACT('month' FROM SETTLEMENTDATE) IN (3, 4, 5) THEN 'Autumn'
                WHEN EXTRACT('month' FROM SETTLEMENTDATE) IN (6, 7, 8) THEN 'Winter'
                ELSE 'Spring'
            END as season,
            EXTRACT('dow' FROM SETTLEMENTDATE) as day_of_week,  -- 0=Sunday, 6=Saturday
            CASE 
                WHEN EXTRACT('dow' FROM SETTLEMENTDATE) IN (0, 6) THEN 'Weekend'
                ELSE 'Weekday'
            END as day_type,
            RRP,
            PERIODID,
            ROW_NUMBER() OVER (
                PARTITION BY DATE_TRUNC('day', SETTLEMENTDATE) 
                ORDER BY SETTLEMENTDATE
            ) as period_of_day
        FROM read_parquet('{PARQUET_DIR}/year={year}/*/REGIONID={region}/*.parquet')
        WHERE RRP IS NOT NULL 
        ORDER BY SETTLEMENTDATE
    ),
    
    rolling_windows AS (
        -- Calculate 2-hour rolling windows with additional metrics
        SELECT 
            trade_date,
            month,
            quarter, 
            season,
            day_type,
            period_of_day,
            SETTLEMENTDATE as window_start,
            
            -- Average price over 2-hour window
            AVG(RRP) OVER (
                PARTITION BY trade_date 
                ORDER BY period_of_day 
                ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING
            ) as window_avg_price,
            
            -- Min and max prices in window (for volatility analysis)
            MIN(RRP) OVER (
                PARTITION BY trade_date 
                ORDER BY period_of_day 
                ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING
            ) as window_min_price,
            
            MAX(RRP) OVER (
                PARTITION BY trade_date 
                ORDER BY period_of_day 
                ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING
            ) as window_max_price,
            
            -- Standard deviation for volatility
            STDDEV(RRP) OVER (
                PARTITION BY trade_date 
                ORDER BY period_of_day 
                ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING
            ) as window_price_volatility,
            
            -- Count periods in window
            COUNT(*) OVER (
                PARTITION BY trade_date 
                ORDER BY period_of_day 
                ROWS BETWEEN CURRENT ROW AND 23 FOLLOWING
            ) as periods_in_window
            
        FROM daily_data
    ),
    
    complete_windows AS (
        SELECT * 
        FROM rolling_windows 
        WHERE periods_in_window = 24  -- Complete 2-hour windows only
    ),
    
    ranked_windows AS (
        SELECT 
            trade_date,
            month,
            quarter,
            season, 
            day_type,
            window_start,
            window_avg_price,
            window_min_price,
            window_max_price,
            window_price_volatility,
            period_of_day,
            
            -- Rank by price for optimal windows
            ROW_NUMBER() OVER (
                PARTITION BY trade_date 
                ORDER BY window_avg_price ASC, period_of_day ASC
            ) as charge_rank,
            
            ROW_NUMBER() OVER (
                PARTITION BY trade_date 
                ORDER BY window_avg_price DESC, period_of_day ASC
            ) as discharge_rank
            
        FROM complete_windows
    ),
    
    daily_optimal AS (
        SELECT 
            trade_date,
            month,
            quarter,
            season,
            day_type,
            
            -- Best charging window
            MAX(CASE WHEN charge_rank = 1 THEN window_start END) as charge_start_time,
            MAX(CASE WHEN charge_rank = 1 THEN window_avg_price END) as charge_avg_price,
            MAX(CASE WHEN charge_rank = 1 THEN window_price_volatility END) as charge_volatility,
            
            -- Best discharging window  
            MAX(CASE WHEN discharge_rank = 1 THEN window_start END) as discharge_start_time,
            MAX(CASE WHEN discharge_rank = 1 THEN window_avg_price END) as discharge_avg_price,
            MAX(CASE WHEN discharge_rank = 1 THEN window_price_volatility END) as discharge_volatility,
            
            -- Daily price statistics
            MIN(window_avg_price) as daily_min_window_price,
            MAX(window_avg_price) as daily_max_window_price,
            AVG(window_avg_price) as daily_avg_window_price,
            STDDEV(window_avg_price) as daily_price_volatility
            
        FROM ranked_windows
        WHERE charge_rank = 1 OR discharge_rank = 1
        GROUP BY trade_date, month, quarter, season, day_type
    )
    
    SELECT 
        trade_date,
        month,
        quarter,
        season,
        day_type,
        charge_start_time,
        charge_start_time + INTERVAL '2 hours' as charge_end_time,
        ROUND(charge_avg_price, 2) as charge_avg_price,
        discharge_start_time,
        discharge_start_time + INTERVAL '2 hours' as discharge_end_time,
        ROUND(discharge_avg_price, 2) as discharge_avg_price,
        ROUND(charge_volatility, 2) as charge_volatility,
        ROUND(discharge_volatility, 2) as discharge_volatility,
        ROUND(daily_price_volatility, 2) as daily_price_volatility
    FROM daily_optimal
    ORDER BY trade_date
    """

# ============================================================================
# ENHANCED FINANCIAL CALCULATIONS
# ============================================================================
def calculate_financial_metrics(df):
    """Calculate comprehensive financial metrics"""
    
    # Basic arbitrage calculations
    df['charging_cost'] = df['charge_avg_price'] * BATTERY_MWH
    df['discharge_revenue'] = df['discharge_avg_price'] * BATTERY_MWH * EFFICIENCY
    df['daily_profit'] = df['discharge_revenue'] - df['charging_cost']
    df['price_spread'] = df['discharge_avg_price'] - df['charge_avg_price']
    
    # Efficiency-adjusted spread
    df['effective_spread'] = df['price_spread'] * EFFICIENCY
    
    # Profitability indicators
    df['is_profitable'] = df['daily_profit'] > 0
    df['meets_threshold'] = df['price_spread'] >= MIN_SPREAD_THRESHOLD
    
    # ROI metrics (daily)
    df['daily_roi_percent'] = (df['daily_profit'] / BATTERY_CAPEX) * 100 * 365  # Annualized daily ROI
    
    # Risk metrics
    df['profit_margin_percent'] = (df['daily_profit'] / df['discharge_revenue']) * 100
    
    return df

# ============================================================================
# REGIONAL ANALYSIS FUNCTION
# ============================================================================
def analyze_region(region):
    """Perform comprehensive analysis for a single region"""
    
    print(f"\n🔍 Analyzing {region}...")
    
    try:
        # Execute query
        query = build_annual_query(region, ANALYSIS_YEAR)
        results = con.execute(query).fetchdf()
        
        if results.empty:
            print(f"   ❌ No data found for {region}")
            return None
        
        # Calculate financial metrics
        results = calculate_financial_metrics(results)
        
        print(f"   ✅ {len(results)} days analyzed")
        return results
        
    except Exception as e:
        print(f"   ❌ Error analyzing {region}: {e}")
        return None

# ============================================================================
# AGGREGATION AND REPORTING FUNCTIONS
# ============================================================================
def create_monthly_summary(df, region):
    """Create monthly aggregated summary"""
    
    monthly = df.groupby(['month']).agg({
        'daily_profit': ['sum', 'mean', 'std', 'min', 'max', 'count'],
        'price_spread': ['mean', 'std'],
        'charge_avg_price': 'mean',
        'discharge_avg_price': 'mean',
        'is_profitable': 'sum',
        'meets_threshold': 'sum',
        'daily_price_volatility': 'mean'
    }).round(2)
    
    # Flatten column names
    monthly.columns = [f"{col[0]}_{col[1]}" for col in monthly.columns]
    
    # Add calculated metrics
    monthly['success_rate_percent'] = (monthly['is_profitable_sum'] / monthly['daily_profit_count'] * 100).round(1)
    monthly['threshold_rate_percent'] = (monthly['meets_threshold_sum'] / monthly['daily_profit_count'] * 100).round(1)
    
    # Add month names
    month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                   7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    monthly['month_name'] = monthly.index.map(month_names)
    
    return monthly

def create_seasonal_summary(df, region):
    """Create seasonal aggregated summary"""
    
    seasonal = df.groupby(['season']).agg({
        'daily_profit': ['sum', 'mean', 'std', 'min', 'max', 'count'],
        'price_spread': ['mean', 'std'],
        'charge_avg_price': 'mean',
        'discharge_avg_price': 'mean',
        'is_profitable': 'sum',
        'daily_price_volatility': 'mean'
    }).round(2)
    
    # Flatten column names
    seasonal.columns = [f"{col[0]}_{col[1]}" for col in seasonal.columns]
    
    # Add success rate
    seasonal['success_rate_percent'] = (seasonal['is_profitable_sum'] / seasonal['daily_profit_count'] * 100).round(1)
    
    return seasonal

def create_annual_summary(regional_data):
    """Create comprehensive annual summary across all regions"""
    
    annual_summary = []
    
    for region, df in regional_data.items():
        if df is None:
            continue
            
        summary = {
            'Region': region,
            'Total_Days': len(df),
            'Total_Profit_$': df['daily_profit'].sum(),
            'Average_Daily_Profit_$': df['daily_profit'].mean(),
            'Best_Day_Profit_$': df['daily_profit'].max(),
            'Worst_Day_Profit_$': df['daily_profit'].min(),
            'Profitable_Days': df['is_profitable'].sum(),
            'Success_Rate_%': (df['is_profitable'].sum() / len(df) * 100),
            'Average_Price_Spread_$/MWh': df['price_spread'].mean(),
            'Average_Daily_Volatility': df['daily_price_volatility'].mean(),
            'Annual_ROI_%': ((df['daily_profit'].sum() - ANNUAL_OPEX) / BATTERY_CAPEX * 100),
            'Payback_Period_Years': BATTERY_CAPEX / (df['daily_profit'].sum() - ANNUAL_OPEX) if (df['daily_profit'].sum() - ANNUAL_OPEX) > 0 else float('inf')
        }
        annual_summary.append(summary)
    
    return pd.DataFrame(annual_summary).round(2)

# ============================================================================
# MAIN EXECUTION - PROCESS ALL REGIONS
# ============================================================================
print("1️⃣ Processing all regions...")

regional_data = {}
for region in REGIONS:
    regional_data[region] = analyze_region(region)

# Filter out regions with no data
valid_regions = {k: v for k, v in regional_data.items() if v is not None}

if not valid_regions:
    print("❌ No data found for any region")
    exit()

print(f"\n✅ Successfully analyzed {len(valid_regions)} regions")

# ============================================================================
# CREATE COMPREHENSIVE REPORTS
# ============================================================================
print("\n2️⃣ Generating comprehensive reports...")

# Create timestamp for file naming
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# ============================================================================
# REPORT 1: ANNUAL SUMMARY (ALL REGIONS)
# ============================================================================
annual_summary = create_annual_summary(regional_data)
annual_file = os.path.join(REPORT_DIR, f"annual_summary_{ANALYSIS_YEAR}_{timestamp}.csv")
annual_summary.to_csv(annual_file, index=False)

print("📊 Annual Summary:")
print(annual_summary[['Region', 'Total_Profit_$', 'Success_Rate_%', 'Annual_ROI_%']].to_string(index=False))

# ============================================================================
# REPORT 2: DETAILED REGIONAL REPORTS
# ============================================================================
for region, df in valid_regions.items():
    # Daily detailed data
    daily_file = os.path.join(REPORT_DIR, "regional", f"daily_analysis_{region}_{ANALYSIS_YEAR}.csv")
    df.to_csv(daily_file, index=False)
    
    # Monthly summary
    monthly_summary = create_monthly_summary(df, region)
    monthly_file = os.path.join(REPORT_DIR, "monthly", f"monthly_summary_{region}_{ANALYSIS_YEAR}.csv")
    monthly_summary.to_csv(monthly_file)
    
    # Seasonal summary
    seasonal_summary = create_seasonal_summary(df, region)
    seasonal_file = os.path.join(REPORT_DIR, f"seasonal_summary_{region}_{ANALYSIS_YEAR}.csv")
    seasonal_summary.to_csv(seasonal_file)

# ============================================================================
# REPORT 3: COMPARATIVE ANALYSIS
# ============================================================================
# Best days across all regions
best_days = []
for region, df in valid_regions.items():
    best_day = df.loc[df['daily_profit'].idxmax()]
    best_day['region'] = region
    best_days.append(best_day)

best_days_df = pd.DataFrame(best_days)
best_days_file = os.path.join(REPORT_DIR, f"best_days_comparison_{ANALYSIS_YEAR}.csv")
best_days_df.to_csv(best_days_file, index=False)

# ============================================================================
# DISPLAY KEY INSIGHTS
# ============================================================================
print(f"\n3️⃣ Key Insights for {ANALYSIS_YEAR}:")
print("-" * 50)

# Overall performance
total_profit = annual_summary['Total_Profit_$'].sum()
best_region = annual_summary.loc[annual_summary['Total_Profit_$'].idxmax()]
avg_success_rate = annual_summary['Success_Rate_%'].mean()

print(f"💰 Total Profit (All Regions): ${total_profit:,.2f}")
print(f"🏆 Best Performing Region: {best_region['Region']} (${best_region['Total_Profit_$']:,.2f})")
print(f"📊 Average Success Rate: {avg_success_rate:.1f}%")

# ROI Analysis
profitable_regions = annual_summary[annual_summary['Annual_ROI_%'] > 0]
print(f"📈 Profitable Regions: {len(profitable_regions)}/{len(annual_summary)}")

if len(profitable_regions) > 0:
    best_roi = profitable_regions.loc[profitable_regions['Annual_ROI_%'].idxmax()]
    print(f"🎯 Best ROI: {best_roi['Region']} ({best_roi['Annual_ROI_%']:.1f}%)")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print(f"\n4️⃣ Files Generated:")
print("-" * 30)
print(f"📄 Annual Summary: {annual_file}")
print(f"📁 Regional Details: {len(valid_regions)} files in /regional/")
print(f"📁 Monthly Summaries: {len(valid_regions)} files in /monthly/")
print(f"📄 Best Days Comparison: {best_days_file}")

print(f"\n🎉 Annual analysis complete!")
print(f"📂 All reports saved to: {REPORT_DIR}")

# Close database connection
con.close()