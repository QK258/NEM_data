import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

st.title("Electricity Price")

# Load Parquet via DuckDB
@st.cache_data
def load_data():
    con = duckdb.connect()
    query = "SELECT * FROM parquet_scan('data/filtered_price_data/**/*.parquet')"
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
agg_option = st.sidebar.selectbox("Aggregation", ["5-minute", "Daily", "Weekly", "Monthly", "Seasonal"])

# Filter
filtered = df[
    (df["REGIONID"] == selected_region) &
    (df["SETTLEMENTDATE"].dt.date >= start_date) &
    (df["SETTLEMENTDATE"].dt.date <= end_date)
].copy()

# Sort by time to ensure correct step plotting
filtered = filtered.sort_values("SETTLEMENTDATE")
# Ensure RRP is numeric
filtered["RRP"] = pd.to_numeric(filtered["RRP"], errors="coerce")

# Aggregation
if agg_option == "Daily":
    filtered = filtered.resample("D", on="SETTLEMENTDATE").agg({"RRP": "mean"}).reset_index()
elif agg_option == "Weekly":
    filtered = filtered.resample("W", on="SETTLEMENTDATE").agg({"RRP": "mean"}).reset_index()
elif agg_option == "Monthly":
    filtered = filtered.resample("M", on="SETTLEMENTDATE").agg({"RRP": "mean"}).reset_index()

# Plotting
if filtered.empty:
    st.warning("No data available for the selected region and date range.")
else:
    if agg_option == "Seasonal":
        filtered["month"] = filtered["SETTLEMENTDATE"].dt.month
        seasonal_avg = filtered.groupby("month")["RRP"].mean().reset_index()
        st.subheader(f"Average RRP by Month in {selected_region}")
        fig = px.bar(
            seasonal_avg,
            x="month",
            y="RRP",
            labels={"RRP": "Price ($/MWh)", "month": "Month"},
            hover_data={"RRP": ':.2f'}
        )
    else:
        x_col = filtered["SETTLEMENTDATE"] if "SETTLEMENTDATE" in filtered else filtered.index
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x_col,
            y=filtered["RRP"],
            mode="lines",
            line_shape="hv",
            name="RRP",
            line=dict(color="crimson"),
            hovertemplate="<b>Time:</b> %{x}<br><b>Price:</b> %{y:,.2f} $/MWh<extra></extra>"
        ))
        fig.update_layout(
            title=f"{agg_option} aggregation of RRP in {selected_region} from {start_date} to {end_date}",
            xaxis_title="Time",
            yaxis_title="Price ($/MWh)",
            height=500
        )

# 📊 Show min, max, avg
if not filtered.empty and "RRP" in filtered.columns:
    max_price = filtered["RRP"].max()
    min_price = filtered["RRP"].min()
    avg_price = filtered["RRP"].mean()

    st.markdown("### Price Statistics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Max Price", f"${max_price:,.2f}")
    col2.metric("Min Price", f"${min_price:,.2f}")
    col3.metric("Average Price", f"${avg_price:,.2f}")

    st.plotly_chart(fig, use_container_width=True)



#streamlit run "C:/Users/user/Google Drive/Projects/Electricity Prices/price_dashboard.py"
