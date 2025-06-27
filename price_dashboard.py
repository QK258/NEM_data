import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go

st.title("Electricity Price Dashboard")

# --- Load data ---
@st.cache_data
def load_data():
    con = duckdb.connect()
    query = "SELECT REGIONID, SETTLEMENTDATE, RRP FROM parquet_scan('data/regional_RRP_data/**/*.parquet', union_by_name=True) " 
    "WHERE SETTLEMENTDATE >= '2025-06-10' AND SETTLEMENTDATE < '2025-06-11' AND REGIONID IS 'NSW1'"
    return con.execute(query).fetchdf()

df = load_data()
df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")

# --- Sidebar ---
regions = df["REGIONID"].dropna().unique().tolist()
selected_regions = st.sidebar.multiselect("Region(s)", regions, default=regions[:5])

min_date = df["SETTLEMENTDATE"].dropna().min()
max_date = df["SETTLEMENTDATE"].dropna().max()
start_date = st.sidebar.date_input("Start date", min_date.date())
end_date = st.sidebar.date_input("End date", max_date.date())

agg_option = st.sidebar.selectbox("Aggregation", ["5-minute", "Daily", "Weekly", "Monthly", "Seasonal"])
price_column = st.sidebar.selectbox("Price Type", [
    "RRP"
])

# --- Filter ---
df[price_column] = pd.to_numeric(df[price_column], errors="coerce")
filtered = df[
    df["REGIONID"].isin(selected_regions) &
    (df["SETTLEMENTDATE"] >= pd.to_datetime(start_date)) &
    (df["SETTLEMENTDATE"] <= pd.to_datetime(end_date))
].copy()

# --- Sort and Aggregate ---
filtered = filtered.sort_values("SETTLEMENTDATE")
if agg_option != "5-minute":
    freq_map = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
    if agg_option in freq_map:
        filtered = filtered.resample(freq_map[agg_option], on="SETTLEMENTDATE").agg({price_column: "mean"}).reset_index()

# --- Stats ---
st.markdown("### Price Statistics")
stats_series = df[
    df["REGIONID"].isin(selected_regions) &
    (df["SETTLEMENTDATE"] >= pd.to_datetime(start_date)) &
    (df["SETTLEMENTDATE"] <= pd.to_datetime(end_date))
][price_column].describe(percentiles=[0.05, 0.5, 0.95])

min_price = stats_series["min"]
max_price = stats_series["max"]
mean_price = stats_series["mean"]
std_price = stats_series["std"]
p5 = stats_series["5%"]
p50 = stats_series["50%"]
p95 = stats_series["95%"]

col1, col2, col3 = st.columns(3)
col1.metric("🔻 Min", f"${min_price:,.2f}")
col2.metric("📊 Avg", f"${mean_price:,.2f}")
col3.metric("🔺 Max", f"${max_price:,.2f}")

col4, col5, col6, col7 = st.columns(4)
col4.metric("5th %tile", f"${p5:,.2f}")
col5.metric("50th %tile", f"${p50:,.2f}")
col6.metric("95th %tile", f"${p95:,.2f}")
col7.metric("Std Dev", f"${std_price:,.2f}")

# --- Plotting ---
if filtered.empty:
    st.warning("No data available for the selected region and date range.")
else:
    fig = go.Figure()
    for region in selected_regions:
        region_df = df[
            (df["REGIONID"] == region) &
            (df["SETTLEMENTDATE"] >= pd.to_datetime(start_date)) &
            (df["SETTLEMENTDATE"] <= pd.to_datetime(end_date))
        ].copy()

        region_df[price_column] = pd.to_numeric(region_df[price_column], errors="coerce")
        region_df = region_df.sort_values("SETTLEMENTDATE")

        if agg_option != "5-minute" and agg_option in freq_map:
            region_df = region_df.resample(freq_map[agg_option], on="SETTLEMENTDATE").agg({price_column: ["mean", lambda x: x.quantile(0.05), lambda x: x.quantile(0.95)]})
            region_df.columns = ["mean", "p05", "p95"]
            region_df = region_df.reset_index()

            fig.add_trace(go.Scatter(x=region_df["SETTLEMENTDATE"], y=region_df["p05"], name=f"{region} 5th %tile", line=dict(width=0.5, dash='dot')))
            fig.add_trace(go.Scatter(x=region_df["SETTLEMENTDATE"], y=region_df["p95"], name=f"{region} 95th %tile", line=dict(width=0.5, dash='dot')))
            fig.add_trace(go.Scatter(x=region_df["SETTLEMENTDATE"], y=region_df["mean"], name=f"{region} mean", line_shape="hv", mode="lines", hovertemplate="%{y:$,.2f}<br>%{x|%H:%M %d/%m/%Y}<extra></extra>"))
        else:
            fig.add_trace(go.Scatter(x=region_df["SETTLEMENTDATE"], y=region_df[price_column], name=f"{region} {price_column}", line_shape="hv", mode="lines"))

    fig.update_layout(
        title=f"{agg_option} {price_column} from {start_date} to {end_date}",
        xaxis_title="Time",
        yaxis_title="Price ($/MWh)",
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Downloads ---
    cols = ["SETTLEMENTDATE", price_column]
    if "REGIONID" in filtered.columns:
        cols.insert(0, "REGIONID")

    download_df = filtered[cols].copy()
    download_df = download_df.rename(columns={price_column: "PRICE"})

    csv = download_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"{agg_option.lower()}_{price_column}_{start_date}_{end_date}.csv",
        mime="text/csv"
    )

#streamlit run "C:/Users/user/Google Drive/Projects/Electricity Prices/price_dashboard.py"
