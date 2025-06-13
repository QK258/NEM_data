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

# Sidebar filters
regions = df["REGIONID"].unique().tolist()
selected_region = st.sidebar.selectbox("Region", regions)
start_date = st.sidebar.date_input("Start date", df["SETTLEMENTDATE"].min().date())
end_date = st.sidebar.date_input("End date", df["SETTLEMENTDATE"].max().date())

# Filter
filtered = df[
    (df["REGIONID"] == selected_region) &
    (df["SETTLEMENTDATE"].dt.date >= start_date) &
    (df["SETTLEMENTDATE"].dt.date <= end_date)
].copy()

# Aggregation options
agg_option = st.sidebar.selectbox("Aggregation", ["5-minute", "Daily", "Weekly", "Monthly", "Seasonal"])

if agg_option == "Daily":
    filtered = filtered.resample("D", on="SETTLEMENTDATE").mean(numeric_only=True).reset_index()
elif agg_option == "Weekly":
    filtered = filtered.resample("W", on="SETTLEMENTDATE").mean(numeric_only=True).reset_index()
elif agg_option == "Monthly":
    filtered = filtered.resample("M", on="SETTLEMENTDATE").mean(numeric_only=True).reset_index()
elif agg_option == "Seasonal":
    filtered["month"] = filtered["SETTLEMENTDATE"].dt.month
    seasonal_avg = filtered.groupby("month")["RRP"].mean().reset_index()
    st.subheader(f"Average RRP by Month in {selected_region}")
    fig = px.bar(seasonal_avg, x="month", y="RRP", labels={"RRP": "Price ($/MWh)", "month": "Month"})
    st.plotly_chart(fig, use_container_width=True)
else:
    # 5-minute (no resampling)
    pass

# Scatter plot for time-based aggregations
if agg_option != "Seasonal":
    if filtered.empty:
        st.warning("No data available for the selected region and date range.")
    else:
        st.subheader(f"{agg_option} RRP in {selected_region}")
        fig = px.scatter(filtered, x="SETTLEMENTDATE", y="RRP",
                         title=f"{agg_option} Spot Prices",
                         labels={"RRP": "Price ($/MWh)"},
                         opacity=0.7)
        fig.update_traces(marker=dict(size=4))
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)


#streamlit run "C:/Users/user/Google Drive/Projects/Electricity Prices/scripts/price_dashboard.py"
