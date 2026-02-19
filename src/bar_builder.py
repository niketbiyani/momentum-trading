"""
BarBuilder: converts a stream of price ticks into OHLCV candlestick bars.

Supports multiple timeframes simultaneously (5s, 15s, 1m).
Maintains a rolling deque of MAX_BARS bars per timeframe per instrument.
"""
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Callable

from src.models import Bar, Tick
from config import MAX_BARS, TIMEFRAMES


class BarBuilder:
    """
    Builds OHLCV bars for a single instrument across multiple timeframes.

    Usage:
        builder = BarBuilder(security_id="12345")
        completed_bar = builder.add_tick(tick)
        bars_1m = builder.get_bars("1m")
    """

    def __init__(
        self,
        security_id: str,
        max_bars: int = MAX_BARS,
        on_bar_close: Optional[Callable] = None,
    ):
        self.security_id = security_id
        self.max_bars = max_bars
        self.on_bar_close = on_bar_close  # callback(security_id, timeframe, bar)
        self._lock = threading.Lock()

        # Per-timeframe state
        self._bars: dict[str, deque] = {tf: deque(maxlen=max_bars) for tf in TIMEFRAMES}
        self._current_bar: dict[str, Optional[Bar]] = {tf: None for tf in TIMEFRAMES}
        self._current_bar_start: dict[str, Optional[datetime]] = {tf: None for tf in TIMEFRAMES}

    # ── Public API ─────────────────────────────────────────────────────────────

    def add_tick(self, tick: Tick) -> dict[str, Optional[Bar]]:
        """
        Process a new tick. Returns a dict of timeframe → completed Bar (or None).
        A completed Bar is returned only for timeframes where a bar just closed.
        """
        completed = {}
        with self._lock:
            for tf, tf_seconds in TIMEFRAMES.items():
                bar_start = self._get_bar_start(tick.timestamp, tf_seconds)
                completed[tf] = self._update_timeframe(tf, bar_start, tick)

        # Fire callbacks AFTER releasing the lock.
        # on_bar_close calls back into bar_builder.get_bars() which also needs
        # self._lock — calling it inside would deadlock (Lock is not reentrant).
        for tf, bar in completed.items():
            if bar and self.on_bar_close:
                self.on_bar_close(self.security_id, tf, bar)

        return completed

    def add_historical_bar(self, timeframe: str, bar: Bar) -> None:
        """Prepend a historical bar (for backfilling from REST API)."""
        with self._lock:
            self._bars[timeframe].appendleft(bar)

    def add_historical_bars(self, timeframe: str, bars: list[Bar]) -> None:
        """Add a list of historical bars in chronological order."""
        with self._lock:
            for bar in bars:
                self._bars[timeframe].append(bar)

    def get_bars(self, timeframe: str) -> list[Bar]:
        """Return a copy of the bar list for the given timeframe (oldest→newest)."""
        with self._lock:
            return list(self._bars[timeframe])

    def get_closes(self, timeframe: str, include_current: bool = True) -> list[float]:
        """
        Return list of close prices (oldest→newest).
        Optionally appends the current (in-progress) bar's last price.
        """
        with self._lock:
            closes = [b.close for b in self._bars[timeframe]]
            if include_current and self._current_bar[timeframe]:
                closes.append(self._current_bar[timeframe].close)
            return closes

    def get_current_bar(self, timeframe: str) -> Optional[Bar]:
        with self._lock:
            return self._current_bar[timeframe]

    def bar_count(self, timeframe: str) -> int:
        with self._lock:
            return len(self._bars[timeframe])

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_bar_start(self, ts: datetime, tf_seconds: int) -> datetime:
        """Floor a timestamp to the nearest bar boundary."""
        epoch = ts.timestamp()
        bar_epoch = int(epoch / tf_seconds) * tf_seconds
        return datetime.fromtimestamp(bar_epoch)

    def _update_timeframe(
        self, tf: str, bar_start: datetime, tick: Tick
    ) -> Optional[Bar]:
        """
        Update the bar for this timeframe with the incoming tick.
        Returns the completed bar if a new bar just started, else None.
        """
        completed_bar = None

        if self._current_bar_start[tf] is None:
            # First tick ever
            self._current_bar_start[tf] = bar_start
            self._current_bar[tf] = Bar(
                timestamp=bar_start,
                open=tick.ltp,
                high=tick.ltp,
                low=tick.ltp,
                close=tick.ltp,
                volume=tick.volume,
            )
        elif bar_start > self._current_bar_start[tf]:
            # New bar started — close the current one
            completed_bar = self._current_bar[tf]
            self._bars[tf].append(completed_bar)
            # (callback fired by add_tick after the lock is released)

            # Open the new bar
            self._current_bar_start[tf] = bar_start
            self._current_bar[tf] = Bar(
                timestamp=bar_start,
                open=tick.ltp,
                high=tick.ltp,
                low=tick.ltp,
                close=tick.ltp,
                volume=tick.volume,
            )
        else:
            # Same bar — update OHLCV
            bar = self._current_bar[tf]
            bar.high = max(bar.high, tick.ltp)
            bar.low = min(bar.low, tick.ltp)
            bar.close = tick.ltp
            bar.volume += tick.volume

        return completed_bar


class MultiInstrumentBarBuilder:
    """
    Manages BarBuilders for multiple instruments.

    Usage:
        mbb = MultiInstrumentBarBuilder(on_bar_close=my_callback)
        mbb.register("12345")
        mbb.on_tick(tick)
    """

    def __init__(self, on_bar_close: Optional[Callable] = None):
        self._builders: dict[str, BarBuilder] = {}
        self._on_bar_close = on_bar_close
        self._lock = threading.Lock()

    def register(self, security_id: str) -> None:
        with self._lock:
            if security_id not in self._builders:
                self._builders[security_id] = BarBuilder(
                    security_id=security_id,
                    on_bar_close=self._on_bar_close,
                )

    def on_tick(self, tick: Tick) -> dict[str, Optional[Bar]]:
        with self._lock:
            builder = self._builders.get(tick.security_id)
        if builder:
            return builder.add_tick(tick)
        return {}

    def get_closes(self, security_id: str, timeframe: str, include_current: bool = True) -> list[float]:
        builder = self._builders.get(security_id)
        if builder:
            return builder.get_closes(timeframe, include_current)
        return []

    def get_bars(self, security_id: str, timeframe: str) -> list[Bar]:
        builder = self._builders.get(security_id)
        if builder:
            return builder.get_bars(timeframe)
        return []

    def get_builder(self, security_id: str) -> Optional[BarBuilder]:
        return self._builders.get(security_id)

    def add_historical_bars(self, security_id: str, timeframe: str, bars: list[Bar]) -> None:
        builder = self._builders.get(security_id)
        if builder:
            builder.add_historical_bars(timeframe, bars)
