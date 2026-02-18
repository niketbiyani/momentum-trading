"""
Technical Indicators Engine

Implements:
  - RSI (14) using Wilder's smoothing
  - EMA(50) applied to the RSI series  → gives "RSI with EMA-50" as the user uses
  - MACD (12, 26, 9) — standard settings
  - Lookback % move table  (10 → 150 bars, step 10)
  - Lookback delta table   (acceleration between consecutive windows)

All functions are stateless — they take a list of close prices and return values.
The IndicatorEngine class maintains state per instrument per timeframe.
"""
import threading
from collections import deque

import numpy as np

from config import (
    RSI_PERIOD,
    RSI_EMA_PERIOD,
    MACD_FAST,
    MACD_SLOW,
    MACD_SIGNAL_PERIOD,
    LOOKBACK_PERIODS,
    MAX_BARS,
)
from src.models import Indicators


# ── Pure calculation functions ─────────────────────────────────────────────────

def _ema(values: np.ndarray, period: int) -> float:
    """Compute the final EMA value from an array using standard multiplier."""
    if len(values) < period:
        return float(values[-1]) if len(values) > 0 else 0.0
    mult = 2.0 / (period + 1)
    ema = float(np.mean(values[:period]))
    for v in values[period:]:
        ema = float(v) * mult + ema * (1 - mult)
    return ema


def _ema_series(values: np.ndarray, period: int) -> np.ndarray:
    """Return the full EMA series for an array."""
    if len(values) == 0:
        return np.array([])
    result = np.empty(len(values))
    result[:period] = np.nan
    if len(values) < period:
        return result
    result[period - 1] = float(np.mean(values[:period]))
    mult = 2.0 / (period + 1)
    for i in range(period, len(values)):
        result[i] = float(values[i]) * mult + result[i - 1] * (1 - mult)
    return result


def calc_rsi(closes: list[float], period: int = RSI_PERIOD) -> float:
    """
    Compute the current RSI value using Wilder's smoothing.
    Returns 50.0 if not enough data.
    """
    if len(closes) < period + 1:
        return 50.0

    arr = np.array(closes, dtype=float)
    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    # Seed with simple average of first `period` deltas
    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))

    # Wilder's smoothing over the rest
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - 100.0 / (1.0 + rs), 2)


def calc_rsi_series(closes: list[float], period: int = RSI_PERIOD) -> list[float]:
    """
    Compute RSI for every bar once enough data is available.
    Returns a list aligned with closes (NaN → 50 for bars without enough history).
    """
    n = len(closes)
    if n < period + 1:
        return [50.0] * n

    arr = np.array(closes, dtype=float)
    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    rsi_vals: list[float] = [50.0] * (period + 1)  # no RSI for first period+1 bars

    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))
    if avg_loss == 0:
        rsi_vals.append(100.0)
    else:
        rsi_vals.append(round(100.0 - 100.0 / (1.0 + avg_gain / avg_loss), 2))

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi_vals.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_vals.append(round(100.0 - 100.0 / (1.0 + rs), 2))

    return rsi_vals


def calc_rsi_ema(closes: list[float], rsi_period: int = RSI_PERIOD, ema_period: int = RSI_EMA_PERIOD) -> float:
    """
    Compute EMA(ema_period) of the RSI series.
    This is the 'RSI with EMA-50' the user refers to.
    """
    rsi_series = calc_rsi_series(closes, rsi_period)
    # Remove the seed 50.0 values from the start (only use real RSI values)
    real_rsi = [v for v in rsi_series if v != 50.0 or len(rsi_series) <= rsi_period + 1]
    if len(real_rsi) == 0:
        return 50.0
    return round(_ema(np.array(real_rsi), ema_period), 2)


def calc_macd(closes: list[float]) -> tuple[float, float, float]:
    """
    Compute MACD (12, 26, 9).
    Returns (macd_line, signal_line, histogram).
    All zero if insufficient data.
    """
    min_bars = MACD_SLOW + MACD_SIGNAL_PERIOD
    if len(closes) < min_bars:
        return 0.0, 0.0, 0.0

    arr = np.array(closes, dtype=float)

    fast_series = _ema_series(arr, MACD_FAST)
    slow_series = _ema_series(arr, MACD_SLOW)

    # MACD line = fast EMA - slow EMA (only where both are valid)
    macd_series = fast_series - slow_series
    # Valid indices start from MACD_SLOW - 1
    valid_start = MACD_SLOW - 1
    valid_macd = macd_series[valid_start:]
    valid_macd = valid_macd[~np.isnan(valid_macd)]

    if len(valid_macd) < MACD_SIGNAL_PERIOD:
        macd_line = float(macd_series[-1]) if not np.isnan(macd_series[-1]) else 0.0
        return round(macd_line, 4), 0.0, round(macd_line, 4)

    signal_line = _ema(valid_macd, MACD_SIGNAL_PERIOD)
    macd_line = float(valid_macd[-1])
    histogram = macd_line - signal_line

    return round(macd_line, 4), round(signal_line, 4), round(histogram, 4)


def calc_lookback_pct(closes: list[float]) -> dict[int, float | None]:
    """
    For each lookback N in LOOKBACK_PERIODS [10, 20, ..., 150]:
      pct[N] = (current_price - price_N_bars_ago) / price_N_bars_ago * 100

    Returns None for periods where there isn't enough bar history.

    KEY INSIGHT for spike detection:
      The difference (pct[N] - pct[N+10]) tells you how much the price moved
      in the 10-bar window that ends N bars ago. A large positive difference
      means an UP spike in that window; a large negative means a DOWN spike.
    """
    current = closes[-1]
    n_bars = len(closes)
    result: dict[int, float | None] = {}

    for n in LOOKBACK_PERIODS:
        if n_bars > n:
            past_price = closes[-(n + 1)]   # price exactly n bars ago
            if past_price > 0:
                pct = (current - past_price) / past_price * 100
                result[n] = round(pct, 2)
            else:
                result[n] = 0.0
        else:
            result[n] = None   # Not enough bars yet

    return result


def calc_lookback_delta(pct_moves: dict[int, float | None]) -> dict[int, float | None]:
    """
    Compute the 'acceleration' (delta) between consecutive lookback windows.

    delta[10]  = pct[10]            — move in the last 10 bars relative to current
    delta[20]  = pct[20] - pct[10]  — move in bars 11–20 (approx)
    delta[30]  = pct[30] - pct[20]  — move in bars 21–30 (approx)
    ...

    A large |delta[N]| indicates a spike occurred in that particular 10-bar window.
    Positive delta → UP spike; negative delta → DOWN spike.

    EXAMPLE (from user): pct[30]=3%, pct[20]=10%
      delta[20] = 10 - 3 = +7% → big UP move happened in bars 11–20 from now
    """
    lookbacks = sorted(LOOKBACK_PERIODS)
    result: dict[int, float | None] = {}
    prev_pct: float = 0.0

    for lb in lookbacks:
        curr = pct_moves.get(lb)
        if curr is None:
            result[lb] = None
        else:
            result[lb] = round(curr - prev_pct, 2)
            prev_pct = curr

    return result


# ── Stateful per-instrument engine ────────────────────────────────────────────

class IndicatorEngine:
    """
    Maintains a rolling close-price buffer and computes all indicators
    on demand for a single (instrument, timeframe) pair.
    """

    def __init__(self, max_bars: int = MAX_BARS):
        self._closes: deque[float] = deque(maxlen=max_bars)
        self._lock = threading.Lock()

    def push_close(self, close: float) -> None:
        with self._lock:
            self._closes.append(close)

    def load_closes(self, closes: list[float]) -> None:
        with self._lock:
            self._closes.clear()
            self._closes.extend(closes)

    def compute(self) -> Indicators:
        with self._lock:
            closes = list(self._closes)

        if not closes:
            return Indicators()

        rsi = calc_rsi(closes)
        rsi_ema = calc_rsi_ema(closes)
        macd_line, macd_signal, macd_hist = calc_macd(closes)
        pct = calc_lookback_pct(closes)
        delta = calc_lookback_delta(pct)

        return Indicators(
            rsi=rsi,
            rsi_ema=rsi_ema,
            macd_line=macd_line,
            macd_signal=macd_signal,
            macd_hist=macd_hist,
            lookback_pct=pct,
            lookback_delta=delta,
        )

    def bar_count(self) -> int:
        with self._lock:
            return len(self._closes)
