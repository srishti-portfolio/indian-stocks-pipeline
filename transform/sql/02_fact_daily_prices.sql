-- =============================================================
-- 02_fact_daily_prices.sql
--
-- Rebuilds fact_daily_prices from raw_prices + dim_stock.
--
-- Strategy: full rebuild each run. With ~10 tickers and a few years of
-- history this is a few thousand rows — trivially fast. It also means
-- if we ever fix a bug in the transform logic, the next run produces
-- corrected history with no manual backfill.
--
-- Derived columns:
--   daily_return_pct  (close - prev_close) / prev_close * 100
--   ma_7              7-day trailing moving average of close
-- =============================================================

-- TRUNCATE is faster than DELETE and resets any auto-vacuum bloat.
TRUNCATE TABLE fact_daily_prices;

INSERT INTO fact_daily_prices (
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    daily_return_pct,
    ma_7
)
SELECT
    r.symbol,
    r.trade_date,
    r.open,
    r.high,
    r.low,
    r.close,
    r.volume,

    -- Daily return: requires the previous trading day's close for the
    -- same symbol. LAG() handles that cleanly. NULL on the first row
    -- per symbol (no previous close exists) — that's correct.
    CASE
        WHEN LAG(r.close) OVER w IS NULL OR LAG(r.close) OVER w = 0
            THEN NULL
        ELSE ROUND(
            ((r.close - LAG(r.close) OVER w) / LAG(r.close) OVER w * 100)::numeric,
            4
        )
    END AS daily_return_pct,

    -- 7-day moving average of close. Uses a window of the prior 6 rows
    -- + current, per symbol, ordered by date. NULL until 7 rows exist
    -- (we use a minimum row count check via COUNT in the same window).
    CASE
        WHEN COUNT(*) OVER w_ma7 < 7 THEN NULL
        ELSE ROUND(AVG(r.close) OVER w_ma7, 4)
    END AS ma_7

FROM raw_prices r
-- Inner join filters out any raw rows whose ticker is no longer tracked
-- in dim_stock (e.g. if you remove a ticker from stocks.yaml).
JOIN dim_stock d ON d.symbol = r.symbol
WINDOW
    w     AS (PARTITION BY r.symbol ORDER BY r.trade_date),
    w_ma7 AS (PARTITION BY r.symbol ORDER BY r.trade_date
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);