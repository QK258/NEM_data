import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
import os

st.title("Electricity Price Dashboard")

# --- Configuration ---
# Set your actual data path here
DATA_PATH = r"C:\Users\user\Google Drive\Projects\Electricity Prices\data\Price_RRP_data"

# Check if data path exists
if not os.path.exists(DATA_PATH):
    st.error(f"Data path not found: {DATA_PATH}")
    st.stop()

# --- Load data with proper error handling ---
@st.cache_data
def load_data():
    try:
        con = duckdb.connect()
        
        # FIXED: Use = instead of IS for string comparison
        # FIXED: Use proper path format for your data
        query = f"""
        SELECT REGIONID, SETTLEMENTDATE, RRP 
        FROM read_parquet('{DATA_PATH}/**/*.parquet') 
        WHERE SETTLEMENTDATE >= '2025-07-01' 
        --AND SETTLEMENTDATE < '2025-07-01' 
        AND REGIONID = 'VIC1'
        ORDER BY SETTLEMENTDATE
        """
        
        df = con.execute(query).fetchdf()
        con.close()
        
        if df.empty:
            st.warning("No data found. Check your data path and date range.")
            return pd.DataFrame()
            
        return df
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Load the data
df = load_data()

if df.empty:
    st.stop()

# Convert date column
df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")

# --- Debug info ---
with st.expander("🔍 Debug Info"):
    st.write(f"Loaded {len(df)} records")
    st.write(f"Date range: {df['SETTLEMENTDATE'].min()} to {df['SETTLEMENTDATE'].max()}")
    st.write(f"Regions in data: {df['REGIONID'].unique().tolist()}")
    st.write(f"Sample data:")
    st.dataframe(df.head())

# --- Sidebar ---
st.sidebar.header("Filters")

# Since we're only loading VIC1, this will only show VIC1
regions = df["REGIONID"].dropna().unique().tolist()
if len(regions) == 1:
    st.sidebar.write(f"Region: **{regions[0]}**")
    selected_regions = regions
else:
    selected_regions = st.sidebar.multiselect("Region(s)", regions, default=regions)

# Date filtering
min_date = df["SETTLEMENTDATE"].dropna().min()
max_date = df["SETTLEMENTDATE"].dropna().max()
start_date = st.sidebar.date_input("Start date", min_date.date())
end_date = st.sidebar.date_input("End date", max_date.date())

# Aggregation options
agg_option = st.sidebar.selectbox("Aggregation", ["5-minute", "Hourly", "Daily", "Weekly"])

# Price column (keeping for future expansion)
price_column = "RRP"

# --- Filter data ---
df[price_column] = pd.to_numeric(df[price_column], errors="coerce")
filtered = df[
    df["REGIONID"].isin(selected_regions) &
    (df["SETTLEMENTDATE"] >= pd.to_datetime(start_date)) &
    (df["SETTLEMENTDATE"] <= pd.to_datetime(end_date))
].copy()

if filtered.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

# --- Aggregation ---
def aggregate_data(data, agg_option):
    """Aggregate data based on the selected option"""
    if agg_option == "5-minute":
        return data
    
    freq_map = {
        "Hourly": "h", 
        "Daily": "D", 
        "Weekly": "W"
    }
    
    if agg_option in freq_map:
        return data.resample(freq_map[agg_option], on="SETTLEMENTDATE").agg({
            price_column: ["mean", "min", "max", "std"]
        }).reset_index()
    
    return data

# Apply aggregation
if agg_option != "5-minute":
    aggregated = aggregate_data(filtered, agg_option)
    # Flatten column names
    aggregated.columns = ['SETTLEMENTDATE', 'mean_price', 'min_price', 'max_price', 'std_price']
else:
    aggregated = filtered

# --- Statistics ---
st.markdown("### 📊 Price Statistics")

stats_series = filtered[price_column].describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])

col1, col2, col3, col4 = st.columns(4)
col1.metric("🔻 Min", f"${stats_series['min']:,.2f}")
col2.metric("📊 Mean", f"${stats_series['mean']:,.2f}")
col3.metric("📈 Median", f"${stats_series['50%']:,.2f}")
col4.metric("🔺 Max", f"${stats_series['max']:,.2f}")

col5, col6, col7, col8 = st.columns(4)
col5.metric("5th %tile", f"${stats_series['5%']:,.2f}")
col6.metric("25th %tile", f"${stats_series['25%']:,.2f}")
col7.metric("75th %tile", f"${stats_series['75%']:,.2f}")
col8.metric("95th %tile", f"${stats_series['95%']:,.2f}")

# --- Time Series Plot ---
st.markdown("### 📈 Price Time Series")

fig = go.Figure()

if agg_option == "5-minute":
    # Plot 5-minute data
    fig.add_trace(go.Scatter(
        x=filtered["SETTLEMENTDATE"], 
        y=filtered[price_column], 
        name=f"{price_column}",
        mode="lines",
        line=dict(width=0.8),
        hovertemplate="<b>%{y:$,.2f}/MWh</b><br>%{x|%H:%M %d/%m/%Y}<extra></extra>"
    ))
else:
    # Plot aggregated data with bands
    fig.add_trace(go.Scatter(
        x=aggregated["SETTLEMENTDATE"], 
        y=aggregated["min_price"], 
        mode="lines",
        line=dict(width=0, color='rgba(0,100,80,0)'),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    fig.add_trace(go.Scatter(
        x=aggregated["SETTLEMENTDATE"], 
        y=aggregated["max_price"], 
        mode="lines",
        line=dict(width=0, color='rgba(0,100,80,0)'),
        fill='tonexty',
        fillcolor='rgba(0,100,80,0.2)',
        name='Min-Max Range',
        hovertemplate="Max: <b>%{y:$,.2f}/MWh</b><br>%{x|%d/%m/%Y}<extra></extra>"
    ))
    
    fig.add_trace(go.Scatter(
        x=aggregated["SETTLEMENTDATE"], 
        y=aggregated["mean_price"], 
        name=f"{agg_option} Average",
        mode="lines+markers",
        line=dict(width=2, color='blue'),
        marker=dict(size=4),
        hovertemplate="<b>%{y:$,.2f}/MWh</b><br>%{x|%d/%m/%Y}<extra></extra>"
    ))

# Add horizontal lines for key statistics
fig.add_hline(y=stats_series['mean'], line_dash="dash", line_color="red", 
              annotation_text=f"Mean: ${stats_series['mean']:.2f}")

fig.update_layout(
    title=f"{agg_option} Electricity Prices - VIC1 ({start_date} to {end_date})",
    xaxis_title="Date/Time",
    yaxis_title="Price ($/MWh)",
    height=600,
    hovermode='x unified'
)

st.plotly_chart(fig, use_container_width=True)

# --- Price Distribution ---
st.markdown("### 📊 Price Distribution")

fig_hist = go.Figure()
fig_hist.add_trace(go.Histogram(
    x=filtered[price_column],
    nbinsx=50,
    name="Price Distribution",
    hovertemplate="Price Range: %{x}<br>Count: %{y}<extra></extra>"
))

fig_hist.update_layout(
    title="VIC1 Price Distribution",
    xaxis_title="Price ($/MWh)",
    yaxis_title="Frequency",
    height=400
)

st.plotly_chart(fig_hist, use_container_width=True)

# --- Downloads ---
st.markdown("### 📥 Download Data")

# Prepare download data
if agg_option == "5-minute":
    download_df = filtered[[ "REGIONID","SETTLEMENTDATE", price_column]].copy()
else:
    download_df = aggregated.copy()
    
download_df = download_df.rename(columns={price_column: "PRICE_$/MWh"})

csv = download_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label=f"📥 Download {agg_option} Data as CSV",
    data=csv,
    file_name=f"prices_{agg_option.lower()}_{start_date}_{end_date}.csv",
    mime="text/csv"
)

# --- Data Summary ---
with st.expander("📋 Data Summary"):
    st.write(f"**Total Records**: {len(filtered):,}")
    st.write(f"**Date Range**: {filtered['SETTLEMENTDATE'].min()} to {filtered['SETTLEMENTDATE'].max()}")
    st.write(f"**Price Range**: ${filtered[price_column].min():.2f} to ${filtered[price_column].max():.2f}")
    st.write(f"**Average Price**: ${filtered[price_column].mean():.2f}")
    
    if len(filtered) > 0:
        negative_prices = len(filtered[filtered[price_column] < 0])
        high_prices = len(filtered[filtered[price_column] > 300])
        st.write(f"**Negative Prices**: {negative_prices} periods ({negative_prices/len(filtered)*100:.1f}%)")
        st.write(f"**High Prices (>$300)**: {high_prices} periods ({high_prices/len(filtered)*100:.1f}%)")


# --- Run the app ---
#streamlit run "C:/Users/user/Google Drive/Projects/Electricity Prices/price_dashboard.py"
