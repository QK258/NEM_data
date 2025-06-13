import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.title("NEM 5-Minute Price Dashboard")

# Load Parquet via DuckDB
@st.cache_data
def load_data():
    con = duckdb.connect()
    query = """
    SELECT * FROM parquet_scan('data/filtered_price_data/**/*.parquet')
    """
    return con.execute(query).fetchdf()

df = load_data()
df["SETTLEMENTDATE"] = pd.to_datetime(
    df["SETTLEMENTDATE"].astype(str).str.strip('"'),
    format="%Y/%m/%d %H:%M:%S",
    errors="coerce"
)
# Ensure SETTLEMENTDATE is in datetime format

# Sidebar filters
regions = df["REGIONID"].unique().tolist()
selected_region = st.sidebar.selectbox("Region", regions)
start_date = st.sidebar.date_input("Start date", df["SETTLEMENTDATE"].min().date())
end_date = st.sidebar.date_input("End date", df["SETTLEMENTDATE"].max().date())

# Filter data
filtered = df[
    (df["REGIONID"] == selected_region) &
    (df["SETTLEMENTDATE"].dt.date >= start_date) &
    (df["SETTLEMENTDATE"].dt.date <= end_date)
]

# Aggregation level
agg_option = st.sidebar.selectbox("Aggregation", ["5-minute", "Daily", "Weekly", "Monthly", "Seasonal"])
if agg_option == "Daily":
    filtered = filtered.resample("D", on="SETTLEMENTDATE").mean()
elif agg_option == "Weekly":
    filtered = filtered.resample("W", on="SETTLEMENTDATE").mean()
elif agg_option == "Monthly":
    filtered = filtered.resample("M", on="SETTLEMENTDATE").mean()
elif agg_option == "Seasonal":
    filtered["month"] = filtered["SETTLEMENTDATE"].dt.month
    filtered = filtered.groupby("month").mean()

# Plot
st.subheader(f"{agg_option} RRP in {selected_region}")
fig = px.line(filtered, x=filtered.index if agg_option != "5-minute" else "SETTLEMENTDATE", y="RRP")
st.plotly_chart(fig, use_container_width=True)
