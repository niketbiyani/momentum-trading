"""
Spike Detector & Signal Engine

Implements the user's strategy:

  STEP 1 — SPIKE detection
    Scan lookback windows [10, 20, 30, 40, 50 bars].
    pct[N] = (current_price - price_N_bars_ago) / price_N_bars_ago * 100
    If the best |pct[N]| ≥ SPIKE_THRESHOLD_PCT → spike detected.
    This compares current price to N bars ago (cumulative), so a 10-bar
    breakout out of a 40-bar consolidation is always captured by pct[10].

  STEP 2 — RSI + MACD confirmation (evaluated immediately)
    UP spike  → RSI ≥ RSI_UP_MIN (45) AND MACD histogram > 0
    DOWN spike → RSI ≤ RSI_DOWN_MAX (55) AND MACD histogram < 0
    If all conditions are met at detection time → emit WATCH directly.
    If not yet met → emit SPIKE (waiting).

  STEP 3 — WATCH signal
    All conditions met → emit WATCH.
    The trader then waits for a small consolidation and enters on the
    breakout of that consolidation's high (UP) or low (DOWN).

Signal lifecycle:
  SPIKE  → large move seen, RSI/MACD not yet confirmed
  WATCH  → spike + RSI + MACD all confirmed (look for breakout entry)
  ENTRY  → breakout confirmed — this is the entry alert
  EXPIRED → conditions broken (RSI crossed boundary or MACD flipped)
"""
import threading
import logging
from collections import deque
from datetime import datetime
from typing import Optional

from config import (
    SPIKE_THRESHOLD_PCT,
    SPIKE_ZSCORE_MIN,
    RSI_OVERBOUGHT,
    RSI_OVERSOLD,
    RSI_UP_MIN,
    RSI_DOWN_MAX,
    LOOKBACK_PERIODS,
)
from src.models import Signal, SignalStatus, Indicators, OptionInfo

logger = logging.getLogger(__name__)

# Minimum z-score for a delta window to be considered a spike.
# A z-score of 2.0 means the concentrated move is 2σ above the 100-bar mean —
# a genuine statistical outlier, not random noise.
_ZSCORE_MIN = SPIKE_ZSCORE_MIN

# Hard-floor: even statistically significant moves must be at least this large
# in absolute % terms (guards against tiny-variance instruments).
_ABS_FLOOR = SPIKE_THRESHOLD_PCT  # default 1.0%


class SpikeState:
    """Tracks the state of an ongoing spike/signal for one (instrument, timeframe)."""

    def __init__(self, direction: str, spike_pct: float, spike_window: int):
        self.direction = direction          # "UP" or "DOWN"
        self.spike_pct = spike_pct         # magnitude of the best pct[N] move
        self.spike_window = spike_window   # lookback window with the largest pct (e.g. 10 = "vs 10 bars ago")
        self.status = SignalStatus.SPIKE
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
                    # If RSI + MACD conditions are already met, jump straight to WATCH
                    if self._all_conditions_met(self._state, indicators):
                        self._state.status = SignalStatus.WATCH
                        self._state.consolidation_high = current_bar_high
                        self._state.consolidation_low = current_bar_low
                        self._state.consol_bars = 1
                        return self._emit(indicators, SignalStatus.WATCH)
                    return self._emit(indicators, SignalStatus.SPIKE)
                return None

            # ── Active state exists ──────────────────────────────────────────
            state = self._state
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
        Detect a statistically significant concentrated spike using z-scores.

        For each of the two most recent 10-bar delta windows, compare the actual
        move to the historical distribution (100-bar rolling baseline):

          delta[10] (window = 10): z-score from ind.spike_zscore
            → move in the LAST 10 bars vs history
          delta[20] (window = 20): z-score from ind.spike_zscore_d20
            → move in bars 11-20 ago vs history

        A z-score >= SPIKE_ZSCORE_MIN (default 2.0) means the move is 2+
        standard deviations above the historical mean — a genuine outlier.
        The hard-floor absolute check (>= 1%) guards against illiquid
        instruments where std is near-zero.

        The staleness guard (pct[10] < 0) has been removed: "as long as
        RSI > 45 for an uptrend we are safe" — if the option has spiked
        11-20 bars ago and RSI is still above 45, the move is still in play.
        The _is_expired() check handles cleanup when conditions break.

        RSI confirmation: RSI must have hit overbought (>=70) or oversold (<=30)
        within the spike window to confirm the move was driven by real momentum.

        Returns (direction, magnitude_pct, lookback_window) or None.
        """
        delta = indicators.lookback_delta

        # Map each delta window to its pre-computed z-score
        window_zscores = {
            10: indicators.spike_zscore,
            20: indicators.spike_zscore_d20,
        }

        best_z      = 0.0
        best_window = 0
        best_delta  = 0.0

        for lb in [10, 20]:
            d = delta.get(lb)
            z = window_zscores.get(lb)
            if d is None or z is None:
                continue
            # Hard-floor: absolute move must be meaningful
            if abs(d) < _ABS_FLOOR:
                continue
            if z > best_z:
                best_z      = z
                best_window = lb
                best_delta  = d

        if best_z < _ZSCORE_MIN:
            return None

        direction = "UP" if best_delta > 0 else "DOWN"

        # RSI must have hit overbought/oversold WITHIN the spike window
        if direction == "UP":
            rsi_peak = indicators.rsi_max_by_window.get(best_window)
            if rsi_peak is None or rsi_peak < RSI_OVERBOUGHT:
                return None
        else:
            rsi_trough = indicators.rsi_min_by_window.get(best_window)
            if rsi_trough is None or rsi_trough > RSI_OVERSOLD:
                return None

        return direction, abs(best_delta), best_window

    def _all_conditions_met(self, state: SpikeState, ind: Indicators) -> bool:
        """RSI is above/below the trend threshold AND MACD confirms direction."""
        if state.direction == "UP":
            return ind.rsi >= RSI_UP_MIN and ind.macd_hist > 0
        else:  # DOWN
            return ind.rsi <= RSI_DOWN_MAX and ind.macd_hist < 0

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
