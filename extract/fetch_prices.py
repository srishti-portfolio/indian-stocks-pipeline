"""
fetch_prices.py — the pipeline entry point.

What this does, in order:
  1. Load config (the basket of tickers).
  2. Make sure tables exist (runs 01_create_tables.sql).
  3. Upsert dim_stock from the YAML config (Python — YAML is source of truth).
  4. For each ticker, figure out the date range to pull (incremental).
  5. Fetch via the data-source abstraction.
  6. Insert-where-not-exists into raw_prices (idempotent).
  7. Rebuild fact_daily_prices from raw_prices + dim_stock.
  8. Run the data-quality checks.

Run it directly:
    python -m extract.fetch_prices
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import text

from extract.data_source import PriceDataSource, YFinanceSource
from load.db import get_engine, run_sql_file, query_df

# ---- Logging setup ---------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_PATH = PROJECT_ROOT / "logs" / "pipeline.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline")


# ---- Config ----------------------------------------------------------

CONFIG_PATH = PROJECT_ROOT / "config" / "stocks.yaml"
SQL_DIR = PROJECT_ROOT / "transform" / "sql"

DEFAULT_BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "365"))
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY_SECONDS", "1.0"))


def load_stock_config() -> list[dict]:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    stocks = config.get("stocks", [])
    if not stocks:
        raise RuntimeError(f"No stocks defined in {CONFIG_PATH}.")
    logger.info("Loaded %d ticker(s) from config.", len(stocks))
    return stocks


# ---- dim_stock from YAML --------------------------------------------

def upsert_dim_stock(stocks: list[dict]) -> int:
    """
    Sync dim_stock from the YAML config. The YAML is the source of truth,
    so we upsert: existing rows get refreshed metadata, new rows are added.
    Returns the number of rows touched.

    We do this in Python rather than SQL because the YAML is what humans
    edit — keeping the loading logic in Python means one source of truth.
    """
    engine = get_engine()
    upsert_sql = text(
        """
        INSERT INTO dim_stock (symbol, company_name, sector, exchange, updated_at)
        VALUES (:symbol, :company_name, :sector, :exchange, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            sector       = EXCLUDED.sector,
            exchange     = EXCLUDED.exchange,
            updated_at   = NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(upsert_sql, stocks)
    logger.info("Upserted %d row(s) into dim_stock.", len(stocks))
    return len(stocks)


# ---- Incremental-load logic ------------------------------------------

def determine_start_date(symbol: str, backfill_days: int) -> date:
    """
    Return the date from which we should fetch new data for `symbol`.

    - If raw_prices already has rows for this symbol, start from the day
      AFTER the latest stored date.
    - Otherwise, this is a first run / backfill — start `backfill_days`
      ago.

    This is what makes the pipeline idempotent and cheap: each run pulls
    only the gap since last time.
    """
    df = query_df(
        "SELECT MAX(trade_date) AS latest FROM raw_prices WHERE symbol = :sym",
        params={"sym": symbol},
    )

    if df.empty or pd.isna(df.iloc[0]["latest"]):
        start = date.today() - timedelta(days=backfill_days)
        logger.info("[%s] First run — backfilling from %s.", symbol, start)
        return start

    latest = df.iloc[0]["latest"]
    start = latest + timedelta(days=1)
    logger.info("[%s] Incremental — fetching from %s.", symbol, start)
    return start


# ---- Insert-where-not-exists -----------------------------------------

def insert_new_rows(df: pd.DataFrame) -> int:
    """
    Insert rows into raw_prices, skipping any that already exist for the
    same (symbol, trade_date). Returns the number of NEW rows inserted.

    Relies on UNIQUE(symbol, trade_date) on raw_prices + ON CONFLICT
    DO NOTHING — re-running the same day is a no-op.
    """
    if df.empty:
        return 0

    engine = get_engine()
    insert_sql = text(
        """
        INSERT INTO raw_prices (symbol, trade_date, open, high, low, close, volume, ingested_at)
        VALUES (:symbol, :trade_date, :open, :high, :low, :close, :volume, NOW())
        ON CONFLICT (symbol, trade_date) DO NOTHING
        """
    )

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        result = conn.execute(insert_sql, records)
        inserted = result.rowcount if result.rowcount is not None else 0

    return inserted


# ---- Main pipeline ---------------------------------------------------

def run(source: PriceDataSource | None = None) -> int:
    """
    Run the full pipeline. Returns total new rows inserted into raw_prices.

    `source` is injectable for testing — defaults to YFinanceSource.
    """
    logger.info("=" * 60)
    logger.info("Pipeline run starting.")
    logger.info("=" * 60)

    source = source or YFinanceSource(request_delay_seconds=REQUEST_DELAY)

    # 1. Ensure tables exist (safe to re-run; uses IF NOT EXISTS).
    logger.info("Ensuring tables exist...")
    run_sql_file(SQL_DIR / "01_create_tables.sql")

    # 2. Load config.
    stocks = load_stock_config()

    # 3. Sync dim_stock from the YAML.
    upsert_dim_stock(stocks)

    # 4. Extract + load, ticker by ticker. Failures on one ticker do NOT
    #    stop the rest — log, continue, surface count at end.
    today = date.today()
    total_inserted = 0
    failures: list[tuple[str, str]] = []

    for stock in stocks:
        symbol = stock["symbol"]
        try:
            start = determine_start_date(symbol, DEFAULT_BACKFILL_DAYS)
            if start > today:
                logger.info("[%s] Already up to date — skipping.", symbol)
                continue

            df = source.fetch(symbol, start=start, end=today)
            inserted = insert_new_rows(df)
            total_inserted += inserted
            logger.info("[%s] Inserted %d new row(s).", symbol, inserted)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[%s] FAILED: %s", symbol, exc)
            failures.append((symbol, str(exc)))

    # 5. Rebuild the clean fact table.
    logger.info("Rebuilding fact_daily_prices...")
    run_sql_file(SQL_DIR / "02_fact_daily_prices.sql")

    # 6. Quality checks. Import here to avoid circular imports at module load.
    logger.info("Running data-quality checks...")
    from quality.checks import run_all_checks
    quality_ok = run_all_checks()

    # 7. Report and set exit code.
    logger.info("=" * 60)
    logger.info("Pipeline run finished.")
    logger.info("  New rows inserted: %d", total_inserted)
    logger.info("  Ticker failures:   %d", len(failures))
    logger.info("  Quality checks:    %s", "PASS" if quality_ok else "FAIL")
    logger.info("=" * 60)

    if failures:
        for sym, msg in failures:
            logger.error("Failed ticker: %s — %s", sym, msg)

    # Exit non-zero if anything went wrong, so GitHub Actions marks it red.
    if failures or not quality_ok:
        sys.exit(1)

    return total_inserted


if __name__ == "__main__":
    run()