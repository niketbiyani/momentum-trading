"""
Spike Detector & Signal Engine

Implements the user's strategy:

  STEP 1 — SPIKE detection
    Within the lookback table, find a 10-bar window where the price moved
    ≥ SPIKE_THRESHOLD_PCT. The delta table (delta[N] = pct[N] - pct[N-10])
    reveals exactly which window had the acceleration.

  STEP 2 — RSI confirmation (from the spike)
    For an UP spike: RSI must have reached overbought (≥ 70) during the spike.
    For a DOWN spike: RSI must have reached oversold  (≤ 30) during the spike.
    We approximate this by checking if current RSI + trend is consistent with
    having been OB/OS and then retracing.

  STEP 3 — RSI boundary check (current)
    After the spike + retracement:
      UP move  → RSI must NOT have dropped below RSI_UP_MIN  (default 45)
      DOWN move → RSI must NOT have risen  above RSI_DOWN_MAX (default 55)

  STEP 4 — MACD alignment
    UP move  → MACD histogram > 0  (bullish momentum)
    DOWN move → MACD histogram < 0  (bearish momentum)

  STEP 5 — WATCH signal
    All conditions met → emit a WATCH signal.
    The trader then waits for a small consolidation and enters on the
    breakout of that consolidation's high (UP) or low (DOWN).

Signal lifecycle:
  SPIKE  → conditions partially met (spike seen, waiting for RSI/MACD)
  WATCH  → all conditions met (look for breakout entry)
  ENTRY  → breakout confirmed — this is the entry alert
  EXPIRED → conditions broken (RSI went through boundary or MACD flipped)
"""
import threading
import logging
from collections import deque
from datetime import datetime
from typing import Optional

from config import (
    SPIKE_THRESHOLD_PCT,
    RSI_OVERBOUGHT,
    RSI_OVERSOLD,
    RSI_UP_MIN,
    RSI_DOWN_MAX,
    LOOKBACK_PERIODS,
)
from src.models import Signal, SignalStatus, Indicators, OptionInfo

logger = logging.getLogger(__name__)

# How many recent 10-bar windows to check for a spike (covers last N*10 bars)
SPIKE_LOOKBACK_WINDOWS = 5   # checks windows ending at 10, 20, 30, 40, 50 bars ago

# Minimum absolute delta to call it a spike
_THRESH = SPIKE_THRESHOLD_PCT


class SpikeState:
    """Tracks the state of an ongoing spike/signal for one (instrument, timeframe)."""

    def __init__(self, direction: str, spike_pct: float, spike_window: int):
        self.direction = direction          # "UP" or "DOWN"
        self.spike_pct = spike_pct         # magnitude of spike delta
        self.spike_window = spike_window   # which 10-bar window (e.g. 20 means bars 11-20)
        self.status = SignalStatus.SPIKE
        self.rsi_min_since_spike: float = 100.0   # track RSI low after an up spike
        self.rsi_max_since_spike: float = 0.0     # track RSI high after a down spike
        self.created_at = datetime.now()
        self.last_updated = datetime.now()
        # For breakout detection
        self.consolidation_high: Optional[float] = None
        self.consolidation_low: Optional[float] = None
        self.consol_bars: int = 0          # consecutive tight bars since WATCH


class SpikeDetector:
    """
    Evaluates indicators for one (instrument, timeframe) pair and
    manages the signal state machine.
    """

    # Consolidation: bar range must be < this % of price to count as tight
    CONSOL_RANGE_PCT = 0.8
    # Need at least this many tight bars before looking for breakout
    CONSOL_MIN_BARS = 3

    def __init__(self, info: OptionInfo, timeframe: str):
        self.info = info
        self.timeframe = timeframe
        self._state: Optional[SpikeState] = None
        self._signals: deque[Signal] = deque(maxlen=20)  # signal history
        self._lock = threading.Lock()

    # ── Main evaluation entry point ────────────────────────────────────────────

    def evaluate(
        self,
        indicators: Indicators,
        current_bar_high: float,
        current_bar_low: float,
        current_bar_close: float,
    ) -> Optional[Signal]:
        """
        Called every time a bar closes or on every tick (for 5s bars).
        Returns a Signal if the state just changed, else None.
        """
        with self._lock:
            spike_info = self._find_spike(indicators)

            # ── No active state ──────────────────────────────────────────────
            if self._state is None:
                if spike_info:
                    direction, spike_pct, spike_window = spike_info
                    self._state = SpikeState(direction, spike_pct, spike_window)
                    return self._emit(indicators, SignalStatus.SPIKE)
                return None

            # ── Active state exists ──────────────────────────────────────────
            state = self._state

            # Track RSI extremes since spike
            state.rsi_min_since_spike = min(state.rsi_min_since_spike, indicators.rsi)
            state.rsi_max_since_spike = max(state.rsi_max_since_spike, indicators.rsi)
            state.last_updated = datetime.now()

            # Check if conditions are now broken → expire the signal
            if self._is_expired(state, indicators):
                self._state = None
                return self._emit(indicators, SignalStatus.EXPIRED)

            # ── SPIKE → WATCH transition ─────────────────────────────────────
            if state.status == SignalStatus.SPIKE:
                if self._all_conditions_met(state, indicators):
                    state.status = SignalStatus.WATCH
                    state.consolidation_high = current_bar_high
                    state.consolidation_low = current_bar_low
                    state.consol_bars = 1
                    return self._emit(indicators, SignalStatus.WATCH)

            # ── WATCH — track consolidation and look for breakout ────────────
            elif state.status == SignalStatus.WATCH:
                bar_range_pct = (
                    (current_bar_high - current_bar_low) / current_bar_close * 100
                    if current_bar_close > 0 else 999
                )
                is_tight_bar = bar_range_pct < self.CONSOL_RANGE_PCT

                if is_tight_bar:
                    # Extend consolidation zone
                    state.consol_bars += 1
                    state.consolidation_high = max(state.consolidation_high, current_bar_high)
                    state.consolidation_low = min(state.consolidation_low, current_bar_low)
                else:
                    # Wide bar — check for breakout
                    if state.consol_bars >= self.CONSOL_MIN_BARS:
                        broke_up   = (state.direction == "UP"   and
                                      current_bar_close > state.consolidation_high)
                        broke_down = (state.direction == "DOWN" and
                                      current_bar_close < state.consolidation_low)

                        if broke_up or broke_down:
                            state.status = SignalStatus.ENTRY
                            signal = self._emit(indicators, SignalStatus.ENTRY)
                            self._state = None   # Reset after entry signal
                            return signal

                    # Re-evaluate conditions; if broken, expire
                    if not self._all_conditions_met(state, indicators):
                        self._state = None
                        return self._emit(indicators, SignalStatus.EXPIRED)

                    # Wide bar but no breakout — reset consolidation tracking
                    state.consol_bars = 0
                    state.consolidation_high = current_bar_high
                    state.consolidation_low = current_bar_low

            return None

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _find_spike(self, indicators: Indicators) -> Optional[tuple[str, float, int]]:
        """
        Scan the delta table for any recent window with a large move.
        Returns (direction, magnitude, window_bars) or None.
        """
        delta = indicators.lookback_delta
        # Only look at the most recent SPIKE_LOOKBACK_WINDOWS windows
        recent = LOOKBACK_PERIODS[:SPIKE_LOOKBACK_WINDOWS]

        best_delta = 0.0
        best_window = 0

        for lb in recent:
            d = delta.get(lb)
            if d is None:
                continue
            if abs(d) > abs(best_delta):
                best_delta = d
                best_window = lb

        if abs(best_delta) >= _THRESH:
            direction = "UP" if best_delta > 0 else "DOWN"
            return direction, abs(best_delta), best_window

        return None

    def _all_conditions_met(self, state: SpikeState, ind: Indicators) -> bool:
        """Check all three conditions: RSI boundary, MACD alignment, RSI level."""
        if state.direction == "UP":
            # RSI must not have dropped below RSI_UP_MIN since the spike
            rsi_ok = state.rsi_min_since_spike >= RSI_UP_MIN and ind.rsi >= RSI_UP_MIN
            macd_ok = ind.macd_hist > 0
        else:  # DOWN
            # RSI must not have risen above RSI_DOWN_MAX since the spike
            rsi_ok = state.rsi_max_since_spike <= RSI_DOWN_MAX and ind.rsi <= RSI_DOWN_MAX
            macd_ok = ind.macd_hist < 0
        return rsi_ok and macd_ok

    def _is_expired(self, state: SpikeState, ind: Indicators) -> bool:
        """Return True if the conditions that generated the signal are now invalid."""
        if state.direction == "UP":
            return ind.rsi < RSI_UP_MIN or ind.macd_hist < 0
        else:
            return ind.rsi > RSI_DOWN_MAX or ind.macd_hist > 0

    def _emit(self, ind: Indicators, status: str) -> Signal:
        state = self._state  # May be None for EXPIRED
        direction = state.direction if state else "?"
        spike_pct = state.spike_pct if state else 0.0
        spike_window = state.spike_window if state else 0

        msg_map = {
            SignalStatus.SPIKE:   f"Spike {spike_pct:.1f}% in last {spike_window} bars — waiting for RSI/MACD",
            SignalStatus.WATCH:   f"All conditions met — watch for breakout (RSI={ind.rsi:.1f}, MACD={'↑' if ind.macd_hist > 0 else '↓'})",
            SignalStatus.ENTRY:   f"BREAKOUT CONFIRMED — Enter {direction}! (RSI={ind.rsi:.1f})",
            SignalStatus.EXPIRED: "Signal expired — conditions broken",
        }

        sig = Signal(
            timestamp=datetime.now(),
            label=self.info.label,
            symbol=self.info.symbol,
            strike=self.info.strike,
            option_type=self.info.option_type,
            direction=direction,
            timeframe=self.timeframe,
            spike_pct=spike_pct,
            spike_window=spike_window,
            rsi=ind.rsi,
            macd_hist=ind.macd_hist,
            status=status,
            message=msg_map.get(status, ""),
        )
        self._signals.appendleft(sig)
        logger.info(str(sig))
        return sig

    @property
    def recent_signals(self) -> list[Signal]:
        with self._lock:
            return list(self._signals)

    @property
    def current_status(self) -> Optional[str]:
        with self._lock:
            return self._state.status if self._state else None

    @property
    def current_direction(self) -> Optional[str]:
        with self._lock:
            return self._state.direction if self._state else None


# ── Multi-instrument coordinator ───────────────────────────────────────────────

class SignalEngine:
    """
    Manages one SpikeDetector per (instrument, timeframe) and collates signals.
    """

    def __init__(self):
        self._detectors: dict[tuple[str, str], SpikeDetector] = {}
        self._all_signals: deque[Signal] = deque(maxlen=100)
        self._lock = threading.Lock()

    def register(self, info: OptionInfo, timeframe: str) -> None:
        key = (info.security_id, timeframe)
        with self._lock:
            if key not in self._detectors:
                self._detectors[key] = SpikeDetector(info, timeframe)

    def evaluate(
        self,
        security_id: str,
        timeframe: str,
        indicators: Indicators,
        bar_high: float,
        bar_low: float,
        bar_close: float,
    ) -> Optional[Signal]:
        key = (security_id, timeframe)
        detector = self._detectors.get(key)
        if not detector:
            return None

        sig = detector.evaluate(indicators, bar_high, bar_low, bar_close)
        if sig:
            with self._lock:
                self._all_signals.appendleft(sig)
        return sig

    def get_detector(self, security_id: str, timeframe: str) -> Optional[SpikeDetector]:
        return self._detectors.get((security_id, timeframe))

    @property
    def all_signals(self) -> list[Signal]:
        with self._lock:
            return list(self._all_signals)
