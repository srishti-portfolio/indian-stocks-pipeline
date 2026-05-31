"""
db.py — database connection and helpers (the "load" layer).

Everything that touches PostgreSQL goes through here, so the rest of the
pipeline never has to think about connections, engines, or credentials.

Works the same whether DATABASE_URL points at Neon, Supabase, or a local
Postgres — only the connection string changes.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load .env when running locally. In GitHub Actions there is no .env file —
# the variables come from repository secrets — and load_dotenv() simply
# does nothing in that case, which is fine.
load_dotenv()

logger = logging.getLogger(__name__)

# Cache the engine module-level so we create exactly one connection pool.
_engine: Engine | None = None


def get_engine() -> Engine:
  """
  Return a singleton SQLAlchemy engine built from DATABASE_URL.

  Raises a clear error if DATABASE_URL is missing — far better than a
  confusing failure deep inside a query later.
  """
  global _engine
  if _engine is not None:
    return _engine

  database_url = os.environ.get("DATABASE_URL")
  if not database_url:
    raise RuntimeError(
      "DATABASE_URL is not set. Copy .env.example to .env and fill it "
      "in (local), or add it as a GitHub repository secret (CI)."
    )

  # pool_pre_ping=True quietly checks a connection is still alive before
  # using it. Cloud free tiers sometimes drop idle connections, and this
  # turns a crash into a transparent reconnect.
  _engine = create_engine(database_url, pool_pre_ping=True)
  logger.info("Database engine created.")
  return _engine


def run_sql_file(path: str | Path) -> None:
  """
  Execute every statement in a .sql file.

  Used to run the files in transform/sql/ in order. Statements are split
  on ';' — fine for this project's straightforward SQL. If you later add
  functions or procedures with embedded semicolons, switch to a smarter
  splitter or run them as one block.
  """
  path = Path(path)
  sql = path.read_text(encoding="utf-8")

  statements = [s.strip() for s in sql.split(";") if s.strip()]

  engine = get_engine()
  with engine.begin() as conn:  # begin() = wrap in a transaction
    for statement in statements:
      conn.execute(text(statement))

  logger.info("Ran SQL file: %s (%d statements)", path.name, len(statements))


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
  """
  Run a SELECT and return the result as a pandas DataFrame.

  Used by the incremental-load logic (to find the latest stored date),
  by the quality checks, and by the dashboard.
  """
  engine = get_engine()
  with engine.connect() as conn:
    return pd.read_sql(text(sql), conn, params=params or {})


def write_df(
  df: pd.DataFrame,
  table_name: str,
  if_exists: str = "append",
) -> int:
  """
  Write a DataFrame to a table. Returns the number of rows written.

  NOTE: this is a plain append. The real duplicate-protection (upsert /
  insert-where-not-exists) belongs in fetch_prices.py once we know the
  exact columns from the API — it is intentionally NOT solved here,
  because doing it properly depends on the real response shape.
  """
  if df.empty:
    logger.warning("write_df called with an empty DataFrame for '%s' — nothing written.", table_name)
    return 0

  engine = get_engine()
  df.to_sql(table_name, engine, if_exists=if_exists, index=False)
  logger.info("Wrote %d rows to '%s'.", len(df), table_name)
  return len(df)


def test_connection() -> bool:
  """
  Quick connectivity check. Handy as the very first thing you run after
  setting up Neon/Supabase, to confirm the connection string works
  before building anything else.

  Run directly:  python -m load.db
  """
  try:
    result = query_df("SELECT 1 AS ok;")
    ok = not result.empty and int(result.iloc[0]["ok"]) == 1
    if ok:
        logger.info("Database connection OK.")
    return ok
  except Exception as exc:  # noqa: BLE001 — we genuinely want any failure reported
    logger.error("Database connection FAILED: %s", exc)
    return False


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
  print("Testing database connection...")
  print("SUCCESS" if test_connection() else "FAILED — check DATABASE_URL in your .env")