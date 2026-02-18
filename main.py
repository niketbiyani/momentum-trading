"""
main.py — Nifty Options Spike Detector

Entry point that wires together:
  - Dhan market feed (websocket ticks)
  - Multi-timeframe bar builder
  - Indicator engine (RSI, MACD, lookback table)
  - Spike detector / signal engine
  - Rich terminal dashboard

Architecture (threads):
  ┌─────────────────────────────────────────────────────┐
  │ feed_thread   DhanFeed.run_forever()                │
  │                 → on_tick() → tick_queue            │
  ├─────────────────────────────────────────────────────┤
  │ main_thread   process loop (100 ms tick)            │
  │                 → drain tick_queue                  │
  │                 → BarBuilder.add_tick()             │
  │                 → IndicatorEngine.compute()         │
  │                 → SpikeDetector.evaluate()          │
  │                 → Dashboard.update_state()          │
  ├─────────────────────────────────────────────────────┤
  │ display_thread  Dashboard.run()                     │
  └─────────────────────────────────────────────────────┘

Usage:
  cp .env.example .env
  # Fill in DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN
  pip install -r requirements.txt
  python main.py
"""
import logging
import queue
import sys
import threading
import time
from collections import deque
from datetime import datetime
from typing import Optional

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler("spike_detector.log")],   # log to file only (don't pollute terminal)
)
logger = logging.getLogger(__name__)

# ── Project imports ────────────────────────────────────────────────────────────
from config import (
    DHAN_CLIENT_ID,
    DHAN_ACCESS_TOKEN,
    TIMEFRAMES,
    DASHBOARD_REFRESH_INTERVAL,
    NIFTY_SECURITY_ID,
)
from src.models import Tick, InstrumentState, OptionInfo
from src.bar_builder import MultiInstrumentBarBuilder
from src.indicators import IndicatorEngine
from src.options_manager import OptionsManager
from src.spike_detector import SignalEngine
from src.dashboard import Dashboard


# ── Dhan exchange / subscription constants ─────────────────────────────────────
# These match the dhanhq.marketfeed module constants
IDX_I   = 0    # NSE Index (Nifty spot)
NSE_FNO = 2    # NSE Futures & Options


# ── Tick queue (feed thread → main thread) ─────────────────────────────────────
_tick_queue: queue.Queue = queue.Queue(maxsize=5000)


# ── Dhan feed integration ──────────────────────────────────────────────────────

def _parse_tick(data: dict) -> Optional[Tick]:
    """
    Convert a raw DhanFeed message dict into our Tick model.

    DhanFeed packet keys (Ticker subscription):
      type, exchange_segment, security_id, LTP, LTT (unix ms), LTQ, volume
    """
    try:
        ltp = float(data.get("LTP", 0) or data.get("ltp", 0))
        if ltp <= 0:
            return None
        sec_id = str(data.get("security_id", data.get("Security Id", "")))
        if not sec_id:
            return None

        # LTT: Dhan sends unix timestamp in seconds or ms
        raw_ltt = data.get("LTT", data.get("ltt", 0)) or 0
        if raw_ltt > 1e12:            # milliseconds
            ts = datetime.fromtimestamp(raw_ltt / 1000)
        elif raw_ltt > 0:             # seconds
            ts = datetime.fromtimestamp(raw_ltt)
        else:
            ts = datetime.now()

        volume = int(data.get("volume", data.get("Volume", 0)) or 0)
        return Tick(timestamp=ts, security_id=sec_id, ltp=ltp, volume=volume)
    except Exception as e:
        logger.debug(f"Tick parse error: {e} | data={data}")
        return None


def _on_message(data: dict) -> None:
    """Callback from DhanFeed websocket thread."""
    tick = _parse_tick(data)
    if tick:
        try:
            _tick_queue.put_nowait(tick)
        except queue.Full:
            pass   # Drop oldest if queue is full (shouldn't happen under normal load)


def _start_dhan_feed(instruments: list[tuple]) -> threading.Thread:
    """
    Start the Dhan websocket feed in a daemon thread.

    instruments: list of (exchange_segment_int, security_id_str, subscription_type_int)
    """
    def _run():
        try:
            from dhanhq import marketfeed
            feed = marketfeed.DhanFeed(
                client_id=DHAN_CLIENT_ID,
                access_token=DHAN_ACCESS_TOKEN,
                instruments=instruments,
                subscription_type=marketfeed.Ticker,
                on_message=_on_message,
            )
            logger.info(f"DhanFeed started with {len(instruments)} instruments")
            feed.run_forever()
        except ImportError:
            logger.error("dhanhq library not installed. Run: pip install dhanhq")
            sys.exit(1)
        except Exception as e:
            logger.error(f"DhanFeed error: {e}", exc_info=True)

    t = threading.Thread(target=_run, name="dhan-feed", daemon=True)
    t.start()
    return t


# ── Historical bar backfill ────────────────────────────────────────────────────

def _backfill_1m_bars(
    dhan_client,
    security_id: str,
    exchange_segment: str,
    bar_builder: MultiInstrumentBarBuilder,
    n_bars: int = 160,
) -> None:
    """
    Fetch recent 1-minute bars from Dhan's REST API and load them into
    the bar builder, providing a warm start for the indicator engine.
    """
    try:
        resp = dhan_client.intraday_minute_data(
            security_id=security_id,
            exchange_segment=exchange_segment,
            instrument_type="OPTIDX",
        )
        data = resp.get("data", {})
        opens  = data.get("open",  [])
        highs  = data.get("high",  [])
        lows   = data.get("low",   [])
        closes = data.get("close", [])
        times  = data.get("start_Time", data.get("timestamp", []))

        from src.models import Bar
        bars = []
        for i in range(min(len(closes), n_bars)):
            ts = datetime.fromtimestamp(times[i]) if times else datetime.now()
            bars.append(Bar(
                timestamp=ts,
                open=float(opens[i]),
                high=float(highs[i]),
                low=float(lows[i]),
                close=float(closes[i]),
                volume=0,
            ))

        bar_builder.add_historical_bars(security_id, "1m", bars)
        logger.info(f"Backfilled {len(bars)} 1m bars for {security_id}")
    except Exception as e:
        logger.warning(f"Backfill failed for {security_id}: {e}")


# ── Main orchestrator ──────────────────────────────────────────────────────────

class SpikeDetectorApp:
    """Wires everything together and runs the main processing loop."""

    def __init__(self):
        self._options_manager = OptionsManager()
        self._bar_builder = MultiInstrumentBarBuilder(on_bar_close=self._on_bar_close)
        self._indicator_engines: dict[tuple[str, str], IndicatorEngine] = {}  # (sec_id, tf) -> engine
        self._signal_engine = SignalEngine()
        self._dashboard = Dashboard()
        self._instrument_states: dict[str, InstrumentState] = {}   # sec_id -> state
        self._running = False

        # For tracking Nifty spot (security_id = NIFTY_SECURITY_ID)
        self._nifty_sec_id = NIFTY_SECURITY_ID

    # ── Startup ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._dashboard.set_status("Loading instruments master…")
        logger.info("Starting Nifty Options Spike Detector")

        # 1. Validate credentials
        if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
            print("\n[ERROR] DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN must be set in .env\n")
            print("  cp .env.example .env")
            print("  # Edit .env and add your credentials\n")
            sys.exit(1)

        # 2. Load instruments master
        if not self._options_manager.initialise():
            logger.warning("Could not load instruments master — will use placeholder security IDs")

        # 3. Get initial Nifty spot (REST API)
        try:
            from dhanhq import dhanhq
            dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
            self._dhan_client = dhan
            logger.info("Dhan REST client initialised")
        except ImportError:
            print("\n[ERROR] dhanhq library not installed. Run: pip install -r requirements.txt\n")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Dhan client init error: {e}")
            print(f"\n[ERROR] Could not initialise Dhan client: {e}\n")
            sys.exit(1)

        self._dashboard.set_status("Fetching Nifty spot price…")

        # 4. Get Nifty spot via quote API
        nifty_spot = self._fetch_nifty_spot()
        if nifty_spot <= 0:
            print("\n[ERROR] Could not fetch Nifty spot price. Check credentials.\n")
            sys.exit(1)

        logger.info(f"Nifty spot: {nifty_spot:.2f}")
        self._dashboard.set_nifty_spot(nifty_spot)

        # 5. Resolve ATM / ITM options
        self._dashboard.set_status("Resolving ATM/ITM options…")
        instruments = self._options_manager.resolve_instruments(nifty_spot)
        if not instruments:
            print("\n[ERROR] Could not resolve option instruments.\n")
            sys.exit(1)

        logger.info(f"Tracking {len(instruments)} options: " +
                    ", ".join(f"{i.label}({i.security_id})" for i in instruments))

        # 6. Register instruments
        for info in instruments:
            self._register_instrument(info)
            # Backfill 1m bars
            _backfill_1m_bars(self._dhan_client, info.security_id, "NSE_FNO",
                               self._bar_builder)

        # Update dashboard with initial states
        self._dashboard.set_instrument_states(list(self._instrument_states.values()))

        # 7. Build feed subscription list
        feed_instruments = [
            (IDX_I, self._nifty_sec_id, 15),      # Nifty spot — Ticker=15
        ]
        for info in instruments:
            feed_instruments.append((NSE_FNO, info.security_id, 15))

        # 8. Start feed
        self._dashboard.set_status("Connecting to Dhan feed…")
        _start_dhan_feed(feed_instruments)
        logger.info("Feed thread started")

        # 9. Start main processing loop in background
        self._running = True
        proc_thread = threading.Thread(target=self._process_loop, name="processor", daemon=True)
        proc_thread.start()

        # 10. Run dashboard in main thread (blocks until 'q')
        self._dashboard.set_status("Live — monitoring for spikes")
        self._dashboard.run(refresh_interval=DASHBOARD_REFRESH_INTERVAL)

        self._running = False
        logger.info("Shutdown complete")

    # ── Instrument registration ────────────────────────────────────────────────

    def _register_instrument(self, info: OptionInfo) -> None:
        sid = info.security_id
        self._bar_builder.register(sid)

        state = InstrumentState(info=info)
        state.bars = {tf: [] for tf in TIMEFRAMES}
        state.indicators = {tf: None for tf in TIMEFRAMES}
        self._instrument_states[sid] = state

        for tf in TIMEFRAMES:
            engine = IndicatorEngine()
            self._indicator_engines[(sid, tf)] = engine
            self._signal_engine.register(info, tf)

    # ── Bar close callback (from BarBuilder) ────────────────────────────────────

    def _on_bar_close(self, security_id: str, timeframe: str, bar) -> None:
        """
        Called from the BarBuilder whenever a new OHLCV bar closes.
        Updates the indicator engine and evaluates signals.
        """
        if security_id == self._nifty_sec_id:
            return   # Nifty spot — we only need LTP, not indicators

        engine = self._indicator_engines.get((security_id, timeframe))
        state = self._instrument_states.get(security_id)
        if not engine or not state:
            return

        # Push close price into indicator engine
        engine.push_close(bar.close)

        # Compute all indicators
        indicators = engine.compute()

        # Update state
        state.indicators[timeframe] = indicators
        state.bars[timeframe] = self._bar_builder.get_bars(security_id, timeframe)

        # Evaluate for signals
        signal = self._signal_engine.evaluate(
            security_id=security_id,
            timeframe=timeframe,
            indicators=indicators,
            bar_high=bar.high,
            bar_low=bar.low,
            bar_close=bar.close,
        )

        if signal:
            # Update active signal on state
            state.active_signals = [signal]
            self._dashboard.push_signal(signal)

        logger.debug(
            f"Bar close {security_id} {timeframe} "
            f"C={bar.close:.2f} RSI={indicators.rsi:.1f} "
            f"MACD={indicators.macd_hist:+.3f}"
        )

    # ── Main processing loop ────────────────────────────────────────────────────

    def _process_loop(self) -> None:
        """Drain the tick queue and dispatch ticks to the bar builder."""
        last_state_push = time.time()

        while self._running:
            # Drain all pending ticks
            processed = 0
            while processed < 200:   # cap per iteration to avoid starving dashboard
                try:
                    tick = _tick_queue.get_nowait()
                except queue.Empty:
                    break

                if tick.security_id == self._nifty_sec_id:
                    # Nifty spot update
                    self._dashboard.set_nifty_spot(tick.ltp)
                    atm_changed = self._options_manager.update_spot(tick.ltp)
                    if atm_changed:
                        logger.info(
                            f"ATM changed to {self._options_manager.current_atm} "
                            f"(Nifty={tick.ltp:.2f}) — consider re-resolving instruments"
                        )
                else:
                    # Option tick
                    state = self._instrument_states.get(tick.security_id)
                    if state:
                        state.prev_ltp = state.ltp
                        state.ltp = tick.ltp
                        state.last_update = tick.timestamp

                    self._bar_builder.on_tick(tick)

                    # Also push to indicator engine for sub-bar updates on current bar
                    # (gives fresher RSI/MACD between bar closes)
                    for tf in TIMEFRAMES:
                        engine = self._indicator_engines.get((tick.security_id, tf))
                        if engine and engine.bar_count() > 0:
                            closes = self._bar_builder.get_closes(
                                tick.security_id, tf, include_current=True
                            )
                            engine.load_closes(closes)

                processed += 1

            # Push updated states to dashboard every 250 ms
            now = time.time()
            if now - last_state_push > 0.25:
                self._dashboard.set_instrument_states(
                    list(self._instrument_states.values())
                )
                last_state_push = now

            time.sleep(0.05)   # 50 ms sleep between tick batches

    # ── Nifty spot fetch ───────────────────────────────────────────────────────

    def _fetch_nifty_spot(self) -> float:
        """
        Attempt to get the Nifty 50 spot price via Dhan's quote API.
        Falls back to 0.0 if unavailable.
        """
        try:
            # Try market_feed quote endpoint (dhanhq v2)
            resp = self._dhan_client.get_market_feed_quote(
                securities={"IDX_I": [int(self._nifty_sec_id)]}
            )
            data = resp.get("data", {}).get("IDX_I", {})
            for sec_id, quote in data.items():
                ltp = float(quote.get("last_price", 0) or quote.get("LTP", 0))
                if ltp > 0:
                    return ltp
        except AttributeError:
            pass
        except Exception as e:
            logger.warning(f"get_market_feed_quote failed: {e}")

        try:
            # Fallback: LTP endpoint
            resp = self._dhan_client.get_last_traded_price(
                securities={"IDX_I": [int(self._nifty_sec_id)]}
            )
            data = resp.get("data", {})
            for key, val in data.items():
                ltp = float(val.get("LTP", 0) or val.get("last_price", 0))
                if ltp > 0:
                    return ltp
        except Exception as e:
            logger.warning(f"get_last_traded_price failed: {e}")

        # As a last resort, return a sensible default so the app can still demo
        logger.warning("Could not fetch Nifty spot — using 22000 as placeholder")
        return 22000.0


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    print("""
╔══════════════════════════════════════════════════════════════════╗
║        NIFTY OPTIONS SPIKE DETECTOR  (v1.0)                     ║
║  Strategy: Impulse spike → RSI retracement → breakout entry     ║
╠══════════════════════════════════════════════════════════════════╣
║  Timeframes : 5s / 15s / 1m                                     ║
║  Instruments: ATM CE, ITM CE, ATM PE, ITM PE (Nifty weekly)     ║
║  Indicators : RSI(14)+EMA(50), MACD(12,26,9), Lookback 10→150   ║
╚══════════════════════════════════════════════════════════════════╝
    """)

    app = SpikeDetectorApp()
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down…")


if __name__ == "__main__":
    main()
