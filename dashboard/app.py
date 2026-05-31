"""
app.py — the Streamlit dashboard.

Reads ONLY from the clean tables (fact_daily_prices, dim_stock). The raw
landing zone is deliberately invisible here — separation of concerns.

Run locally:
  streamlit run dashboard/app.py

Deploy to Streamlit Community Cloud:
  Point it at this file in your GitHub repo, and add DATABASE_URL as a
  secret in the Streamlit deployment settings.
"""
from __future__ import annotations
import os 
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import streamlit as st

# Bridge Streamlit Cloud secrets into os.environ BEFORE importing
# load.db. On Streamlit Cloud, secrets live in st.secrets, not in the
# process environment — without this, load.db raises "DATABASE_URL is
# not set". Locally (via .env) and in GitHub Actions (via the env: block
# in the workflow) os.environ is already populated, so this is a no-op.
try:
    for key, value in st.secrets.items():
        os.environ.setdefault(key, str(value))
except (FileNotFoundError, st.errors.StreamlitSecretNotFoundError):
    # No secrets.toml present — running locally with .env. Fine.
    pass

from load.db import query_df

# ---- Page setup ------------------------------------------------------

st.set_page_config(
  page_title="Indian Stocks Pipeline",
  page_icon="📈",
  layout="wide",
)

st.title("📈 Indian Stocks — Pipeline Dashboard")
st.caption(
  "Daily NSE prices, ingested via yfinance → PostgreSQL (Neon) → "
  "SQL transforms → this dashboard. Refreshed daily by GitHub Actions."
)


# ---- Cached data loaders --------------------------------------------
# st.cache_data avoids hammering Neon on every interaction.
# 10-minute TTL is plenty for daily data.

@st.cache_data(ttl=600)
def load_stocks() -> pd.DataFrame:
  return query_df("SELECT symbol, company_name, sector, exchange FROM dim_stock ORDER BY company_name")


@st.cache_data(ttl=600)
def load_prices(symbols: tuple[str, ...], start: str, end: str) -> pd.DataFrame:
  if not symbols:
    return pd.DataFrame()
  # SQLAlchemy + a tuple param needs the IN-clause expansion idiom.
  placeholders = ", ".join(f":sym{i}" for i in range(len(symbols)))
  params = {f"sym{i}": s for i, s in enumerate(symbols)}
  params["start"] = start
  params["end"] = end
  sql = f"""
    SELECT f.symbol, d.company_name, d.sector, f.trade_date, f.open, f.high, f.low, f.close, f.volume, f.daily_return_pct, f.ma_7
    FROM fact_daily_prices f
    JOIN dim_stock d ON d.symbol = f.symbol
    WHERE f.symbol IN ({placeholders})
      AND f.trade_date BETWEEN :start AND :end
    ORDER BY f.symbol, f.trade_date
  """
  return query_df(sql, params=params)


# ---- Sidebar: filters ------------------------------------------------

stocks_df = load_stocks()
if stocks_df.empty:
  st.warning("No data yet. Run the pipeline (`python -m extract.fetch_prices`) to populate the database.")
  st.stop()

st.sidebar.header("Filters")

# Sector filter (multi-select)
all_sectors = sorted(stocks_df["sector"].unique().tolist())
selected_sectors = st.sidebar.multiselect("Sectors", all_sectors, default=all_sectors)

filtered_stocks = stocks_df[stocks_df["sector"].isin(selected_sectors)]

# Ticker filter (multi-select, populated from sector selection)
ticker_labels = [f"{r.company_name} ({r.symbol})" for r in filtered_stocks.itertuples()]
label_to_symbol = dict(zip(ticker_labels, filtered_stocks["symbol"]))

default_labels = ticker_labels[: min(5, len(ticker_labels))]
selected_labels = st.sidebar.multiselect("Tickers", ticker_labels, default=default_labels)
selected_symbols = tuple(label_to_symbol[l] for l in selected_labels)

# Date-range filter
date_bounds = query_df("SELECT MIN(trade_date) AS d_min, MAX(trade_date) AS d_max FROM fact_daily_prices")
d_min = date_bounds.iloc[0]["d_min"]
d_max = date_bounds.iloc[0]["d_max"]

date_range = st.sidebar.date_input(
  "Date range",
  value=(d_min, d_max),
  min_value=d_min,
  max_value=d_max,
)
if isinstance(date_range, tuple) and len(date_range) == 2:
  start_date, end_date = date_range
else:
  start_date, end_date = d_min, d_max


# ---- Main panel ------------------------------------------------------

if not selected_symbols:
  st.info("Pick at least one ticker from the sidebar.")
  st.stop()

prices = load_prices(selected_symbols, str(start_date), str(end_date))

if prices.empty:
  st.info("No data for the chosen filters.")
  st.stop()

# --- KPI row ---
latest_per_symbol = prices.sort_values("trade_date").groupby("symbol").tail(1)
total_tickers = len(latest_per_symbol)
avg_daily_return = prices["daily_return_pct"].dropna().mean()
latest_date = prices["trade_date"].max()

k1, k2, k3 = st.columns(3)
k1.metric("Tickers shown", total_tickers)
k2.metric(
  "Avg daily return %",
  f"{avg_daily_return:.2f}%" if pd.notna(avg_daily_return) else "—",
)
k3.metric("Latest trade date", str(latest_date))

st.divider()

# --- Closing prices over time ---
st.subheader("Closing prices over time")
fig_close = px.line(
  prices,
  x="trade_date",
  y="close",
  color="company_name",
  labels={"trade_date": "Date", "close": "Close (INR)", "company_name": "Stock"},
)
fig_close.update_layout(legend_title_text="")
st.plotly_chart(fig_close, use_container_width=True)

# --- Daily return distribution ---
st.subheader("Daily return distribution")
fig_ret = px.box(
  prices.dropna(subset=["daily_return_pct"]),
  x="company_name",
  y="daily_return_pct",
  labels={"company_name": "Stock", "daily_return_pct": "Daily return %"},
)
fig_ret.update_layout(xaxis_title="", showlegend=False)
st.plotly_chart(fig_ret, use_container_width=True)

# --- Latest snapshot table ---
st.subheader("Latest snapshot")
snapshot = (
  latest_per_symbol
  .loc[:, ["symbol", "company_name", "sector", "trade_date", "close", "daily_return_pct", "ma_7", "volume"]]
  .rename(columns={
    "symbol": "Symbol",
    "company_name": "Company",
    "sector": "Sector",
    "trade_date": "Date",
    "close": "Close",
    "daily_return_pct": "Return %",
    "ma_7": "MA (7d)",
    "volume": "Volume",
  })
  .reset_index(drop=True)
)
st.dataframe(snapshot, use_container_width=True, hide_index=True)

st.caption(
  "Built as a portfolio project: scheduled ingestion (yfinance), "
  "dimensional modelling, data-quality checks, and this dashboard. "
  "Source on GitHub."
)