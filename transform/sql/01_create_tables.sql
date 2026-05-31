-- =============================================================
-- 01_create_tables.sql
--
-- Creates the three tables the pipeline needs. Idempotent: safe to run
-- on every pipeline invocation (uses IF NOT EXISTS everywhere).
--
-- Tables:
--   raw_prices         landing zone, untouched after insert
--   dim_stock          the dimension table (one row per ticker)
--   fact_daily_prices  the clean fact table (rebuilt each run)
-- =============================================================


-- ---- raw_prices --------------------------------------------------
-- Verbatim landing zone for whatever the data source returns.
-- The UNIQUE(symbol, trade_date) constraint is what makes
-- INSERT ... ON CONFLICT DO NOTHING work in fetch_prices.py —
-- it is the mechanism behind idempotent re-runs.

CREATE TABLE IF NOT EXISTS raw_prices (
  id           BIGSERIAL PRIMARY KEY,
  symbol       TEXT        NOT NULL,
  trade_date   DATE        NOT NULL,
  open         NUMERIC(14, 4),
  high         NUMERIC(14, 4),
  low          NUMERIC(14, 4),
  close        NUMERIC(14, 4),
  volume       BIGINT,
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_raw_prices_symbol_date
  ON raw_prices (symbol, trade_date);


-- ---- dim_stock ---------------------------------------------------
-- Dimension table. Built from config/stocks.yaml by 02_dim_stock.sql.
-- Small, semi-hand-curated — provides the names, sectors, and exchange
-- info the dashboard needs to make raw tickers human-readable.

CREATE TABLE IF NOT EXISTS dim_stock (
  symbol        TEXT PRIMARY KEY,
  company_name  TEXT NOT NULL,
  sector        TEXT NOT NULL,
  exchange      TEXT NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ---- fact_daily_prices -------------------------------------------
-- The clean, analytics-ready fact table. One row per (symbol, trade_date).
--
-- Built by 03_fact_daily_prices.sql from raw_prices + dim_stock.
-- The dashboard reads ONLY from this table — keeping derived metrics
-- here means the dashboard stays simple and fast.
--
-- Derived columns:
--   daily_return_pct   (close - prev_close) / prev_close * 100
--   ma_7               7-day moving average of close

CREATE TABLE IF NOT EXISTS fact_daily_prices (
  symbol            TEXT NOT NULL REFERENCES dim_stock(symbol),
  trade_date        DATE NOT NULL,
  open              NUMERIC(14, 4),
  high              NUMERIC(14, 4),
  low               NUMERIC(14, 4),
  close             NUMERIC(14, 4),
  volume            BIGINT,
  daily_return_pct  NUMERIC(10, 4),
  ma_7              NUMERIC(14, 4),
  PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_fact_daily_prices_date
  ON fact_daily_prices (trade_date);