"""
data_source.py — the data-source abstraction layer.

Why this exists
---------------
The pipeline talks to *some* external source for OHLCV data. Today that
source is yfinance (the Yahoo Finance Python library). Tomorrow it could
be a REST API, a paid feed, or a fallback when one fails.

Rather than hard-wire yfinance everywhere, the rest of the pipeline talks
to the abstract `PriceDataSource` interface. Swapping the source is then
a matter of writing one new class — no changes to fetch_prices, no
changes to the SQL, no changes to the dashboard.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


# ---- The interface ----------------------------------------------------

class PriceDataSource(ABC):
    """
    Anything that can return daily OHLCV for a ticker over a date range.

    Implementations MUST return a DataFrame with these exact columns:

        symbol      str       e.g. "RELIANCE.NS"
        trade_date  date      Python date, no time component
        open        float
        high        float
        low         float
        close       float
        volume      int

    Implementations MUST drop rows where any of the OHLC values is null.
    """

    @abstractmethod
    def fetch(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        """Return OHLCV rows for `symbol` from `start` to `end` (inclusive)."""
        ...


# ---- The yfinance implementation --------------------------------------

class YFinanceSource(PriceDataSource):
    """
    Pulls daily OHLCV from Yahoo Finance via the yfinance library.

    Uses yf.download() rather than Ticker.history() because download() hits
    a different Yahoo endpoint that is currently far less prone to the
    rate-limit / empty-JSON-response issue that Ticker.history triggers.

    Handles three real-world quirks:
      1. Yahoo intermittently returns empty responses. We retry once with
         a back-off before giving up on a ticker.
      2. yfinance returns a partial NaN row for the current day during
         market hours. We drop NaN-OHLC rows after fetching.
      3. yfinance uses capitalised columns and a timezone-aware index.
         We normalise both before returning.
    """

    def __init__(self, request_delay_seconds: float = 2.5, max_attempts: int = 3):
        # 2.5s delay between tickers — Yahoo's free endpoints are aggressive
        # about throttling burst traffic. Slower is more reliable.
        self.request_delay_seconds = request_delay_seconds
        self.max_attempts = max_attempts

    def fetch(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        logger.info("Fetching %s from %s to %s ...", symbol, start, end)

        history = self._download_with_retry(symbol, start, end)

        # Be polite to Yahoo before the next ticker.
        time.sleep(self.request_delay_seconds)

        if history is None or history.empty:
            logger.warning("No data returned for %s after %d attempt(s).",
                           symbol, self.max_attempts)
            return self._empty_frame()

        df = history.reset_index()

        # yf.download can return MultiIndex columns when given a single
        # ticker — flatten that case to a simple Index.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        # --- Normalise columns ---
        df = df.rename(columns={
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })

        required = ["trade_date", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.warning("Missing columns for %s: %s. Got: %s",
                           symbol, missing, list(df.columns))
            return self._empty_frame()

        df = df[required].copy()

        # --- Drop partial NaN-OHLC rows ---
        before = len(df)
        df = df.dropna(subset=["open", "high", "low", "close"])
        dropped = before - len(df)
        if dropped:
            logger.info("Dropped %d partial row(s) with NaN OHLC for %s.",
                        dropped, symbol)

        if df.empty:
            return self._empty_frame()

        # --- Date: strip time + timezone ---
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

        # --- Volume: force int ---
        df["volume"] = df["volume"].astype("int64")

        # --- Tag with symbol ---
        df.insert(0, "symbol", symbol)

        return df.reset_index(drop=True)

    def _download_with_retry(self, symbol: str, start: date, end: date):
        """
        Wrap yf.download in retry-on-empty logic. Yahoo intermittently
        returns empty responses; one retry catches most transient cases
        without making the pipeline noticeably slower.
        """
        # yf.download's `end` is exclusive — add a day to include `end`.
        end_exclusive = (pd.Timestamp(end) + pd.Timedelta(days=1)).date()

        for attempt in range(1, self.max_attempts + 1):
            try:
                df = yf.download(
                    tickers=symbol,
                    start=start.isoformat(),
                    end=end_exclusive.isoformat(),
                    progress=False,         # silence yfinance's progress bar
                    auto_adjust=True,       # fold splits/dividends into Close
                    threads=False,          # single ticker, no need to parallelise
                )
                if df is not None and not df.empty:
                    return df
                logger.info("Empty response for %s (attempt %d/%d).",
                            symbol, attempt, self.max_attempts)
            except Exception as exc:  # noqa: BLE001 — log and retry
                logger.warning("yfinance error for %s (attempt %d/%d): %s",
                               symbol, attempt, self.max_attempts, exc)

            if attempt < self.max_attempts:
                # Exponential back-off: 3s, 6s, ...
                time.sleep(3 * attempt)

        return None

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        """A correctly-shaped empty frame, so callers can treat it uniformly."""
        return pd.DataFrame(
            columns=["symbol", "trade_date", "open", "high", "low", "close", "volume"]
        )