# Indian Stock Market Data Pipeline

> An end-to-end data pipeline that ingests daily NSE/BSE market data on a
> schedule, stores it in cloud PostgreSQL, transforms it with SQL into
> analytics-ready tables, validates it with quality checks, and serves
> it through a live Streamlit dashboard.

**Live dashboard:** _<link once deployed>_

---

## What this project does and why

This project demonstrates end-to-end data-engineering fundamentals:
scheduled ingestion, raw/clean separation, dimensional modelling,
data-quality validation, and a deployed dashboard. The pipeline runs
unattended on a daily schedule via GitHub Actions, fetches price data
for a basket of NSE-listed stocks, lands it in a Postgres landing zone,
transforms it with SQL into a clean fact/dimension model with derived
metrics, runs quality checks that fail the build on stale or missing
data, and exposes it through an interactive Streamlit dashboard.

## Architecture

```
yfinance (NSE tickers, .NS suffix)
        |
        v
Extract (Python)  --->  raw_prices            (landing zone, immutable)
        |
        v
Transform (SQL)   --->  dim_stock              (dimension, upserted from YAML)
                        fact_daily_prices      (clean fact + derived metrics)
        |
        v
Quality checks    --->  emptiness, coverage, freshness, nulls, sanity, uniqueness
        |
        v
Streamlit dashboard  (reads only the clean tables)

Orchestrated by GitHub Actions on a weekday schedule (19:30 IST).
```

## Tech stack and why each piece

| Layer        | Choice                       | Why |
|--------------|------------------------------|-----|
| Ingestion    | Python + `yfinance`          | Stable Yahoo-backed source; NSE/BSE via `.NS`/`.BO` suffix |
| Database     | PostgreSQL (Neon)            | Industry-standard SQL; free cloud tier reachable by CI + dashboard |
| Transform    | SQL (window functions)       | The right tool for set-based modelling and derived metrics |
| Orchestration| GitHub Actions (cron)        | Free, cloud-hosted, no local machine required |
| Quality      | Python checks                | Fails the build on stale, missing, or malformed data |
| Dashboard    | Streamlit + Plotly           | Fast to build, deploys free, single-language repo |

## Data model

Star-schema-lite: one fact table, one dimension.

- **`raw_prices`** — verbatim landing zone for each yfinance pull.
  Never modified after insert, so the clean layer can always be
  rebuilt from raw if transform logic changes.
  Unique constraint on `(symbol, trade_date)` powers idempotent inserts.
- **`dim_stock`** — dimension: symbol, company name, sector, exchange.
  Upserted from `config/stocks.yaml` on every run — the YAML is the
  source of truth for what's tracked.
- **`fact_daily_prices`** — clean fact table: one row per symbol per
  trading day, joined to `dim_stock`, with derived metrics:
  - `daily_return_pct` — day-over-day percentage change
  - `ma_7` — 7-day trailing moving average of close
  Fully rebuilt each run, which keeps history correct if the transform
  is ever fixed.

## Design decisions worth highlighting

- **Swappable data source.** Extraction sits behind a `PriceDataSource`
  interface in `extract/data_source.py`. The current implementation is
  `YFinanceSource`; swapping to a REST API or a paid feed is one new
  class, no changes elsewhere.
- **Incremental loading.** Each run queries the latest `trade_date`
  already stored per ticker, then fetches only the gap. First run does
  a backfill (configurable via `BACKFILL_DAYS`).
- **Idempotency.** `INSERT ... ON CONFLICT (symbol, trade_date) DO NOTHING`
  on `raw_prices` means re-running the pipeline the same day is a no-op.
- **Raw / clean separation.** Raw data is immutable. All transformation
  logic lives in SQL files under `transform/sql/`.
- **Defensive extract layer.** Drops partial NaN-OHLC rows (a yfinance
  intraday quirk), normalises column names and types, and continues
  past individual ticker failures rather than aborting the whole run.
- **Data-quality gate.** Six checks (emptiness, ticker coverage,
  freshness, no-null OHLC, high≥low, no duplicates) run after every
  pipeline invocation. Failures exit non-zero so the GitHub Actions
  run shows red — silent staleness is the failure mode this guards
  against.

## Data source evaluation

The data source was chosen after evaluating three options:

1. **Finnhub REST API.** Free tier doesn't cover Indian exchanges;
   ruled out.
2. **Community NSE/BSE REST APIs.** The candidates found were either
   unmaintained scrapers of NSE pages (likely to break without
   warning) or documentation-only repos pointing at a single
   hobby-hosted server with no SLA. Too fragile for a pipeline
   meant to run for months unattended.
3. **yfinance.** Library, not REST. Backed by Yahoo Finance, which
   has served Indian market data reliably for years. Chosen for
   stability while structuring the extract layer so a REST source
   could be added later without rewrites.

## Running it locally

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # Mac/Linux
# .venv\Scripts\Activate.ps1         # Windows PowerShell

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
#    fill in DATABASE_URL with your Neon/Supabase connection string

# 4. Confirm the database connection works
python -m load.db

# 5. Run the pipeline
python -m extract.fetch_prices

# 6. Launch the dashboard
streamlit run dashboard/app.py
```

## Project structure

```
indian-stocks-pipeline/
├── config/          # stocks.yaml — the tracked basket
├── extract/         # data-source abstraction + extraction orchestration
├── transform/sql/   # SQL: create tables, build fact_daily_prices
├── load/            # database connection + helpers
├── quality/         # data-quality validations
├── dashboard/       # Streamlit app
├── logs/            # pipeline logs (gitignored)
└── .github/         # scheduled GitHub Actions workflow
```

## Limitations and what I'd do next

- **No automated tests yet.** The next addition would be unit tests for
  `data_source.py` (mocking yfinance) and integration tests for the SQL
  transforms against a temporary Postgres.
- **Full-rebuild transform.** `fact_daily_prices` is rebuilt each run.
  Trivial at this scale; an incremental transform via `dbt` would be
  the natural next step.
- **No alerting.** Quality failures show as red Actions runs; piping
  them to email or Slack on failure would be a small lift.
- **Single source.** Adding a fallback `PriceDataSource` (e.g. a REST
  API) and wiring the orchestrator to try one then the other would
  harden the pipeline against Yahoo outages.

---

_Built as a portfolio project to demonstrate end-to-end data engineering:
scheduled ingestion, dimensional modelling, data quality, and a deployed
dashboard._