"""
main.py — Options Spike Detector (Nifty 50 Stocks Edition)

Entry point that wires together:
  - Dhan market feed (websocket ticks for 50 stocks + their options)
  - Multi-timeframe bar builder
  - Indicator engine (RSI, MACD, lookback table)
  - Spike detector / signal engine
  - FastAPI web dashboard (replaces terminal UI)

Architecture (threads):
  ┌─────────────────────────────────────────────────────┐
  │ web-server thread   uvicorn (FastAPI + WebSocket)   │
  ├─────────────────────────────────────────────────────┤
  │ dhan-feed thread    DhanFeed.run_forever()          │
  │                       → on_tick() → tick_queue      │
  ├─────────────────────────────────────────────────────┤
  │ processor thread    100ms loop                      │
  │                       → drain tick_queue            │
  │                       → BarBuilder.on_tick()        │
  │                       → IndicatorEngine.compute()   │
  │                       → SpikeDetector.evaluate()    │
  │                       → web.server.update_state()   │
  └─────────────────────────────────────────────────────┘

Usage:
  cp .env.example .env
  # Fill in DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN
  pip install -r requirements.txt
  python main.py
  # Open http://localhost:8000 in your browser
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
    handlers=[logging.FileHandler("spike_detector.log")],
)
logger = logging.getLogger(__name__)

# ── Project imports ────────────────────────────────────────────────────────────
from config import (
    DHAN_CLIENT_ID,
    DHAN_ACCESS_TOKEN,
    TIMEFRAMES,
    NIFTY_SECURITY_ID,
    NIFTY50_STOCKS,
    BACKFILL_STOCK_OPTIONS,
    WEB_HOST,
    WEB_PORT,
)
from src.models import Tick, InstrumentState, OptionInfo, Signal
from src.bar_builder import MultiInstrumentBarBuilder
from src.indicators import IndicatorEngine
from src.stock_manager import StockOptionsManager
from src.spike_detector import SignalEngine
from web.server import start_server, update_state

# ── Dhan exchange segment constants ───────────────────────────────────────────
IDX_I   = 0    # NSE Index
NSE_EQ  = 1    # NSE Equity (stock spot prices)
NSE_FNO = 2    # NSE Futures & Options

# ── Tick queue (feed thread → processor thread) ────────────────────────────────
_tick_queue: queue.Queue = queue.Queue(maxsize=10_000)


# ── Tick parsing ──────────────────────────────────────────────────────────────

def _parse_tick(data: dict) -> Optional[Tick]:
    try:
        ltp = float(data.get("LTP", 0) or data.get("ltp", 0))
        if ltp <= 0:
            return None
        sec_id = str(data.get("security_id", data.get("Security Id", "")))
        if not sec_id:
            return None
        raw_ltt = data.get("LTT", data.get("ltt", 0)) or 0
        if raw_ltt > 1e12:
            ts = datetime.fromtimestamp(raw_ltt / 1000)
        elif raw_ltt > 0:
            ts = datetime.fromtimestamp(raw_ltt)
        else:
            ts = datetime.now()
        volume = int(data.get("volume", data.get("Volume", 0)) or 0)
        return Tick(timestamp=ts, security_id=sec_id, ltp=ltp, volume=volume)
    except Exception as e:
        logger.debug(f"Tick parse error: {e}")
        return None


def _on_message(data: dict) -> None:
    tick = _parse_tick(data)
    if tick:
        try:
            _tick_queue.put_nowait(tick)
        except queue.Full:
            pass


# ── Feed startup ──────────────────────────────────────────────────────────────

def _start_dhan_feed(instruments: list[tuple]) -> threading.Thread:
    def _run():
        try:
            from dhanhq import marketfeed
            feed = marketfeed.DhanFeed(
                client_id=DHAN_CLIENT_ID,
                access_token=DHAN_ACCESS_TOKEN,
                instruments=instruments,
                on_message=_on_message,
            )
            logger.info(f"DhanFeed started with {len(instruments)} instruments")
            feed.run_forever()
        except ImportError:
            logger.error("dhanhq not installed. Run: pip install -r requirements.txt")
            sys.exit(1)
        except Exception as e:
            logger.error(f"DhanFeed error: {e}", exc_info=True)

    t = threading.Thread(target=_run, name="dhan-feed", daemon=True)
    t.start()
    return t


# ── Historical backfill ───────────────────────────────────────────────────────

def _backfill_1m(dhan_client, security_id: str, exchange_segment: str,
                 bar_builder: MultiInstrumentBarBuilder, n_bars: int = 160) -> None:
    try:
        instr_type = "OPTIDX" if exchange_segment == "NSE_FNO" else "OPTSTK"
        resp = dhan_client.intraday_minute_data(
            security_id=security_id,
            exchange_segment=exchange_segment,
            instrument_type=instr_type,
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
        logger.debug(f"Backfill failed for {security_id}: {e}")


# ── Main app ──────────────────────────────────────────────────────────────────

class SpikeDetectorApp:
    """Wires all components together and runs the processing loop."""

    def __init__(self):
        self._stock_manager = StockOptionsManager()
        self._bar_builder = MultiInstrumentBarBuilder(on_bar_close=self._on_bar_close)
        self._indicator_engines: dict[tuple[str, str], IndicatorEngine] = {}
        self._signal_engine = SignalEngine()
        self._instrument_states: dict[str, InstrumentState] = {}
        self._signals: deque[Signal] = deque(maxlen=100)
        self._running = False

        # Security ID maps
        self._eq_sec_to_symbol: dict[str, str] = {}   # NSE_EQ sec_id -> stock symbol
        self._stock_spots: dict[str, float] = {}       # stock symbol -> spot price
        self._status_msg: str = "Starting…"

    # ── Startup ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║     OPTIONS SPIKE DETECTOR  —  Nifty 50 Stocks Edition          ║
║  Strategy: Impulse spike → RSI retracement → breakout entry     ║
╠══════════════════════════════════════════════════════════════════╣
║  Instruments : ATM CE + ATM PE for all {len(NIFTY50_STOCKS)} Nifty 50 stocks   ║
║  Timeframes  : 5s / 15s / 1m                                    ║
║  Dashboard   : http://{WEB_HOST if WEB_HOST != '0.0.0.0' else 'localhost'}:{WEB_PORT}                             ║
╚══════════════════════════════════════════════════════════════════╝
""")

        if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
            print("[ERROR] Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env\n")
            sys.exit(1)

        # 1. Start web server first so the dashboard is immediately accessible
        self._set_status("Starting web server…")
        start_server(host=WEB_HOST, port=WEB_PORT)
        time.sleep(0.5)   # give uvicorn a moment to bind
        print(f"  Dashboard: http://localhost:{WEB_PORT}")

        # 2. Load instruments master
        self._set_status("Loading instruments master…")
        if not self._stock_manager.initialise():
            logger.warning("Instruments master load failed — will use placeholders")

        # 3. Init Dhan REST client
        self._set_status("Connecting to Dhan API…")
        try:
            from dhanhq import dhanhq
            self._dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        except ImportError:
            print("[ERROR] dhanhq not installed. Run: pip install -r requirements.txt")
            sys.exit(1)
        except Exception as e:
            print(f"[ERROR] Dhan client init failed: {e}")
            sys.exit(1)

        # 4. Fetch spot prices for all 50 stocks
        self._set_status("Fetching spot prices for Nifty 50 stocks…")
        self._stock_spots = self._stock_manager.fetch_spot_prices(self._dhan)
        if not self._stock_spots:
            logger.warning("No spot prices fetched — using placeholder strikes")
            # Provide some default prices so the rest can proceed
            self._stock_spots = {sym: 1000.0 for sym in NIFTY50_STOCKS}

        logger.info(f"Got spot prices for {len(self._stock_spots)} stocks")

        # 5. Resolve options for all stocks
        self._set_status("Resolving ATM options for all stocks…")
        instruments, eq_ids = self._stock_manager.resolve_all(self._stock_spots)

        if not instruments:
            print("[ERROR] Could not resolve any option instruments.")
            sys.exit(1)

        logger.info(f"Tracking {len(instruments)} option instruments")

        # 6. Register instruments
        for info in instruments:
            self._register_instrument(info)

        # 7. Build reverse map: NSE_EQ security_id -> symbol
        for symbol, sec_id in eq_ids.items():
            self._eq_sec_to_symbol[sec_id] = symbol

        # 8. Backfill historical bars (optional — can slow startup)
        if BACKFILL_STOCK_OPTIONS:
            self._set_status("Backfilling historical bars…")
            for info in instruments:
                _backfill_1m(self._dhan, info.security_id, "NSE_FNO", self._bar_builder)

        # 9. Build WebSocket subscription list
        feed_instruments: list[tuple] = []
        # Stock spot feeds (NSE_EQ)
        for symbol, sec_id in eq_ids.items():
            feed_instruments.append((NSE_EQ, sec_id, 15))
        # Options feeds (NSE_FNO)
        for info in instruments:
            if not info.security_id.startswith(info.underlying or ""):
                # Only subscribe real security IDs (skip placeholders)
                feed_instruments.append((NSE_FNO, info.security_id, 15))

        # 10. Start Dhan feed
        self._set_status("Connecting to Dhan market feed…")
        _start_dhan_feed(feed_instruments)
        logger.info(f"Feed started — {len(feed_instruments)} subscriptions")

        # 11. Start processor
        self._running = True
        proc = threading.Thread(target=self._process_loop, name="processor", daemon=True)
        proc.start()

        self._set_status("Live — monitoring for spikes")
        print("  Press Ctrl+C to stop.\n")

        # 12. Block main thread until interrupted
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down…")
            self._running = False

    # ── Instrument registration ────────────────────────────────────────────────

    def _register_instrument(self, info: OptionInfo) -> None:
        sid = info.security_id
        self._bar_builder.register(sid)
        state = InstrumentState(info=info)
        state.bars = {tf: [] for tf in TIMEFRAMES}
        state.indicators = {tf: None for tf in TIMEFRAMES}
        self._instrument_states[sid] = state
        for tf in TIMEFRAMES:
            self._indicator_engines[(sid, tf)] = IndicatorEngine()
            self._signal_engine.register(info, tf)

    # ── Bar close callback ─────────────────────────────────────────────────────

    def _on_bar_close(self, security_id: str, timeframe: str, bar) -> None:
        engine = self._indicator_engines.get((security_id, timeframe))
        state = self._instrument_states.get(security_id)
        if not engine or not state:
            return

        engine.push_close(bar.close)
        indicators = engine.compute()

        state.indicators[timeframe] = indicators
        state.bars[timeframe] = self._bar_builder.get_bars(security_id, timeframe)

        signal = self._signal_engine.evaluate(
            security_id=security_id,
            timeframe=timeframe,
            indicators=indicators,
            bar_high=bar.high,
            bar_low=bar.low,
            bar_close=bar.close,
        )
        if signal:
            signal.underlying = state.info.underlying
            state.active_signals = [signal]
            self._signals.appendleft(signal)

    # ── Processing loop ────────────────────────────────────────────────────────

    def _process_loop(self) -> None:
        last_state_push = time.time()

        while self._running:
            processed = 0
            while processed < 300:
                try:
                    tick = _tick_queue.get_nowait()
                except queue.Empty:
                    break

                # NSE_EQ tick → update stock spot price
                if tick.security_id in self._eq_sec_to_symbol:
                    sym = self._eq_sec_to_symbol[tick.security_id]
                    self._stock_spots[sym] = tick.ltp
                else:
                    # Options tick
                    state = self._instrument_states.get(tick.security_id)
                    if state:
                        state.prev_ltp = state.ltp
                        state.ltp = tick.ltp
                        state.last_update = tick.timestamp
                    self._bar_builder.on_tick(tick)

                    for tf in TIMEFRAMES:
                        engine = self._indicator_engines.get((tick.security_id, tf))
                        if engine and engine.bar_count() > 0:
                            closes = self._bar_builder.get_closes(
                                tick.security_id, tf, include_current=True
                            )
                            engine.load_closes(closes)

                processed += 1

            # Push state to web dashboard every 500ms
            now = time.time()
            if now - last_state_push > 0.5:
                update_state(self._build_web_state())
                last_state_push = now

            time.sleep(0.05)

    # ── State serialisation for web dashboard ─────────────────────────────────

    def _build_web_state(self) -> dict:
        """Build a JSON-serialisable state snapshot for the web dashboard."""
        stocks_data: dict[str, dict] = {}

        for sid, state in self._instrument_states.items():
            underlying = state.info.underlying or state.info.symbol
            opt_type = state.info.option_type   # "CE" or "PE"

            if underlying not in stocks_data:
                stocks_data[underlying] = {
                    "spot": self._stock_spots.get(underlying, 0),
                    "options": {},
                }
            else:
                stocks_data[underlying]["spot"] = self._stock_spots.get(underlying,
                    stocks_data[underlying]["spot"])

            # Per-timeframe indicators
            indicators_by_tf: dict[str, dict] = {}
            for tf in TIMEFRAMES:
                ind = state.indicators.get(tf)
                if ind:
                    indicators_by_tf[tf] = {
                        "rsi":       round(ind.rsi, 1),
                        "rsi_ema":   round(ind.rsi_ema, 1),
                        "macd_hist": round(ind.macd_hist, 4),
                        "bars":      len(state.bars.get(tf, [])),
                    }

            active_sig = state.active_signals[-1] if state.active_signals else None
            stocks_data[underlying]["options"][opt_type] = {
                "symbol":         state.info.symbol,
                "strike":         state.info.strike,
                "ltp":            state.ltp,
                "ltp_change_pct": round(state.ltp_change_pct, 2),
                "indicators":     indicators_by_tf,
                # Flat (current 1m values) for backwards compat
                "rsi":       round((state.indicators.get("1m") or _empty_ind()).rsi, 1),
                "macd_hist": round((state.indicators.get("1m") or _empty_ind()).macd_hist, 4),
                "bars":      len(state.bars.get("1m", [])),
                "signal_status":    active_sig.status    if active_sig else None,
                "signal_direction": active_sig.direction if active_sig else None,
            }

        signals_data = [
            {
                "timestamp":  s.timestamp.strftime("%H:%M:%S"),
                "underlying": s.underlying or s.label,
                "stock":      s.underlying or s.label,
                "label":      s.label,
                "direction":  s.direction,
                "timeframe":  s.timeframe,
                "status":     s.status,
                "message":    s.message,
                "spike_pct":  round(s.spike_pct, 1),
            }
            for s in list(self._signals)[:50]
        ]

        return {
            "timestamp":  datetime.now().strftime("%H:%M:%S"),
            "status":     self._status_msg,
            "active_tf":  "1m",
            "stocks":     stocks_data,
            "signals":    signals_data,
        }

    def _set_status(self, msg: str) -> None:
        self._status_msg = msg
        update_state({"status": msg, "stocks": {}, "signals": [],
                      "timestamp": datetime.now().strftime("%H:%M:%S"),
                      "active_tf": "1m"})
        logger.info(msg)


def _empty_ind():
    from src.models import Indicators
    return Indicators()


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    app = SpikeDetectorApp()
    app.start()


if __name__ == "__main__":
    main()
