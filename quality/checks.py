"""
checks.py — data-quality validations.

Runs at the end of every pipeline invocation. If a check fails, the
pipeline exits non-zero so the GitHub Actions run is flagged red — which
is the whole point: a portfolio dashboard that silently goes stale is
worse than no dashboard at all.

Each check returns a (passed: bool, message: str) tuple. run_all_checks
runs the whole battery, logs each result, and returns True only if all
checks passed.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from load.db import query_df

logger = logging.getLogger("pipeline.quality")

# How recent the latest trade_date must be to count as "fresh".
# India markets close Sat/Sun and on holidays, so we allow some slack.
# 7 days handles a long weekend + a national holiday cluster.
FRESHNESS_THRESHOLD_DAYS = 7


# ---- Individual checks ------------------------------------------------

def check_fact_table_not_empty() -> tuple[bool, str]:
  df = query_df("SELECT COUNT(*) AS n FROM fact_daily_prices")
  n = int(df.iloc[0]["n"])
  if n == 0:
      return False, "fact_daily_prices is empty — pipeline produced no clean data."
  return True, f"fact_daily_prices has {n:,} rows."


def check_every_ticker_has_data() -> tuple[bool, str]:
  """
  Every ticker in dim_stock should have at least one row in the fact
  table. If one is missing, extraction silently lost a ticker — bad.
  """
  df = query_df(
    """
    SELECT d.symbol
    FROM dim_stock d
    LEFT JOIN fact_daily_prices f ON f.symbol = d.symbol
    WHERE f.symbol IS NULL
    """
  )
  if not df.empty:
    missing = ", ".join(df["symbol"].tolist())
    return False, f"Tickers in dim_stock with no fact data: {missing}"
  return True, "Every ticker has at least one fact row."


def check_freshness() -> tuple[bool, str]:
  """
  The latest trade_date should be recent. If it's stale, either the
  market has been closed for an unusually long time, or extraction
  has been silently failing.
  """
  df = query_df("SELECT MAX(trade_date) AS latest FROM fact_daily_prices")
  latest = df.iloc[0]["latest"]
  if latest is None:
    return False, "No trade_date found — fact table is empty."

  age_days = (date.today() - latest).days
  threshold = FRESHNESS_THRESHOLD_DAYS
  if age_days > threshold:
    return False, f"Latest trade_date is {latest} ({age_days} days old, > {threshold})."
  return True, f"Latest trade_date is {latest} ({age_days} days old)."


def check_no_null_ohlc() -> tuple[bool, str]:
  """
  fact_daily_prices should never contain NULL OHLC values.
  data_source.py drops partial rows on extract — if any leaked through,
  something is wrong upstream.
  """
  df = query_df(
    """
    SELECT COUNT(*) AS n
    FROM fact_daily_prices
    WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
    """
  )
  n = int(df.iloc[0]["n"])
  if n > 0:
    return False, f"{n} row(s) in fact_daily_prices have NULL OHLC values."
  return True, "No NULL OHLC values in fact_daily_prices."


def check_high_low_sanity() -> tuple[bool, str]:
  """
  For any given day, high >= low must hold. Violations indicate a data
  corruption issue upstream — has never happened with yfinance in
  practice, but a cheap check worth having.
  """
  df = query_df(
    """
    SELECT COUNT(*) AS n
    FROM fact_daily_prices
    WHERE high < low
    """
  )
  n = int(df.iloc[0]["n"])
  if n > 0:
    return False, f"{n} row(s) have high < low — data corruption."
  return True, "high >= low holds across all rows."


def check_no_duplicate_rows() -> tuple[bool, str]:
  """
  (symbol, trade_date) is the PK on fact_daily_prices so this is enforced
  by the DB — but a defensive check costs nothing and catches schema
  regressions if someone ever drops the constraint.
  """
  df = query_df(
    """
    SELECT COUNT(*) AS n
    FROM (
      SELECT symbol, trade_date
      FROM fact_daily_prices
      GROUP BY symbol, trade_date
      HAVING COUNT(*) > 1
    ) AS dupes
    """
  )
  n = int(df.iloc[0]["n"])
  if n > 0:
    return False, f"{n} (symbol, trade_date) combinations are duplicated."
  return True, "No duplicate (symbol, trade_date) rows."


# ---- Runner ----------------------------------------------------------

ALL_CHECKS = [
  ("fact_table_not_empty",   check_fact_table_not_empty),
  ("every_ticker_has_data",  check_every_ticker_has_data),
  ("freshness",              check_freshness),
  ("no_null_ohlc",           check_no_null_ohlc),
  ("high_low_sanity",        check_high_low_sanity),
  ("no_duplicate_rows",      check_no_duplicate_rows),
]


def run_all_checks() -> bool:
  """Run every check, log results, return True iff all passed."""
  logger.info("-" * 60)
  logger.info("Data-quality checks")
  logger.info("-" * 60)

  all_passed = True
  for name, check_fn in ALL_CHECKS:
    try:
      passed, message = check_fn()
    except Exception as exc:  # noqa: BLE001 — quality check itself failed
      logger.error("  [ERROR] %s — %s", name, exc)
      all_passed = False
      continue

    status = "PASS" if passed else "FAIL"
    logger.info("  [%s] %s — %s", status, name, message)
    if not passed:
      all_passed = False

  logger.info("-" * 60)
  return all_passed


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
  ok = run_all_checks()
  print("ALL PASSED" if ok else "SOME CHECKS FAILED")