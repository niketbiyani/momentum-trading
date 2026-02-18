"""
main.py — Options Spike Detector (Nifty Index + Nifty 50 Stocks)

Entry point that wires together:
  - Dhan market feed (websocket ticks for Nifty index, 50 stocks + their options)
  - Multi-timeframe bar builder
  - Indicator engine (RSI, MACD, lookback table)
  - Spike detector / signal engine
  - FastAPI web dashboard with two tabs: Nifty | Nifty 50

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
from src.options_manager import OptionsManager
from src.stock_manager import StockOptionsManager, fetch_nifty_spot
from src.spike_detector import SignalEngine
from web.server import start_server, update_state, get_active_tf

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
        self._stock_manager  = StockOptionsManager()
        self._nifty_manager  = OptionsManager()
        self._bar_builder    = MultiInstrumentBarBuilder(on_bar_close=self._on_bar_close)
        self._indicator_engines: dict[tuple[str, str], IndicatorEngine] = {}
        self._signal_engine  = SignalEngine()
        self._instrument_states: dict[str, InstrumentState] = {}
        self._signals: deque[Signal] = deque(maxlen=100)
        self._running = False

        # Security ID maps
        self._eq_sec_to_symbol: dict[str, str] = {}   # NSE_EQ sec_id -> stock symbol
        self._stock_spots: dict[str, float] = {}       # stock symbol -> spot price
        self._nifty_spot: float = 0.0

        # Per-timeframe signals: (security_id, timeframe) -> Signal
        self._tf_signals: dict[tuple[str, str], Signal] = {}

        self._status_msg: str = "Starting…"

    # ── Startup ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║        OPTIONS SPIKE DETECTOR  —  Nifty + Nifty 50              ║
║  Strategy: Impulse spike → RSI retracement → breakout entry     ║
╠══════════════════════════════════════════════════════════════════╣
║  Tab 1 : Nifty index (ATM CE / ITM CE / ATM PE / ITM PE)        ║
║  Tab 2 : ATM CE + ATM PE for {len(NIFTY50_STOCKS)} Nifty 50 stocks         ║
║  Timeframes  : 5s / 15s / 1m                                    ║
║  Dashboard   : http://localhost:{WEB_PORT}                             ║
╚══════════════════════════════════════════════════════════════════╝
""")

        if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
            print("[ERROR] Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env\n")
            sys.exit(1)

        # 1. Start web server first
        self._set_status("Starting web server…")
        start_server(host=WEB_HOST, port=WEB_PORT)
        time.sleep(0.5)
        print(f"  Dashboard: http://localhost:{WEB_PORT}")

        # 2. Load instruments master (shared by both managers via disk cache)
        self._set_status("Loading instruments master…")
        if not self._stock_manager.initialise():
            logger.warning("Instruments master (stock) load failed — using placeholders")
        if not self._nifty_manager.initialise():
            logger.warning("Instruments master (nifty) load failed — using placeholders")

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

        # 4. Fetch Nifty index spot price
        self._set_status("Fetching Nifty index spot price…")
        self._nifty_spot = fetch_nifty_spot(self._dhan)
        if self._nifty_spot <= 0:
            logger.warning("Nifty spot unavailable at startup — will update from feed")
            # Use a reasonable fallback so we can at least subscribe to something
            self._nifty_spot = 23000.0

        # 5. Resolve Nifty index options (ATM CE, ITM CE, ATM PE, ITM PE)
        self._set_status("Resolving Nifty ATM options…")
        nifty_instruments = self._nifty_manager.resolve_instruments(
            spot_price=self._nifty_spot
        )
        for info in nifty_instruments:
            info.underlying = "NIFTY"
            self._register_instrument(info)

        logger.info(
            f"Nifty instruments: {[i.label for i in nifty_instruments]} "
            f"(spot={self._nifty_spot}, ATM={self._nifty_manager.current_atm})"
        )

        # 6. Fetch spot prices for Nifty 50 stocks
        self._set_status("Fetching spot prices for Nifty 50 stocks…")
        self._stock_spots = self._stock_manager.fetch_spot_prices(self._dhan)
        if not self._stock_spots:
            logger.warning("No stock spot prices fetched — using placeholders")
            self._stock_spots = {sym: 1000.0 for sym in NIFTY50_STOCKS}

        logger.info(f"Got spot prices for {len(self._stock_spots)} stocks")

        # 7. Resolve options for all Nifty 50 stocks
        self._set_status("Resolving ATM options for Nifty 50 stocks…")
        stock_instruments, eq_ids = self._stock_manager.resolve_all(self._stock_spots)
        for info in stock_instruments:
            self._register_instrument(info)

        logger.info(f"Tracking {len(stock_instruments)} stock option instruments")

        # 8. Build reverse map: NSE_EQ security_id -> symbol
        for symbol, sec_id in eq_ids.items():
            self._eq_sec_to_symbol[sec_id] = symbol

        # 9. Optional backfill
        if BACKFILL_STOCK_OPTIONS:
            self._set_status("Backfilling historical bars…")
            for info in stock_instruments:
                if info.security_id.isdigit():
                    _backfill_1m(self._dhan, info.security_id, "NSE_FNO", self._bar_builder)

        # 10. Build WebSocket subscription list
        feed_instruments: list[tuple] = [
            # Nifty 50 index (spot)
            (IDX_I, NIFTY_SECURITY_ID, 15),
        ]
        # Nifty index options
        for info in nifty_instruments:
            if info.security_id.isdigit():
                feed_instruments.append((NSE_FNO, info.security_id, 15))
        # Stock equities (spot feeds)
        for symbol, sec_id in eq_ids.items():
            feed_instruments.append((NSE_EQ, sec_id, 15))
        # Stock options
        for info in stock_instruments:
            if info.security_id.isdigit():
                feed_instruments.append((NSE_FNO, info.security_id, 15))

        # 11. Start Dhan feed
        self._set_status("Connecting to Dhan market feed…")
        _start_dhan_feed(feed_instruments)
        logger.info(f"Feed started — {len(feed_instruments)} subscriptions")

        # 12. Start processor
        self._running = True
        proc = threading.Thread(target=self._process_loop, name="processor", daemon=True)
        proc.start()

        self._set_status("Live — monitoring for spikes")
        print("  Press Ctrl+C to stop.\n")

        # 13. Block main thread
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
        state  = self._instrument_states.get(security_id)
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
            self._tf_signals[(security_id, timeframe)] = signal
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

                if tick.security_id == NIFTY_SECURITY_ID:
                    # Nifty index spot tick
                    self._nifty_spot = tick.ltp
                elif tick.security_id in self._eq_sec_to_symbol:
                    # Stock equity spot tick → update spot price
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
        # Read active_tf BEFORE replacing the shared state so the user's
        # timeframe selection is preserved across state updates.
        active_tf = get_active_tf()

        nifty_data:  dict = {"spot": round(self._nifty_spot, 2), "options": {}}
        stocks_data: dict = {}

        for sid, state in self._instrument_states.items():
            underlying = state.info.underlying or state.info.symbol

            # Per-timeframe indicators + per-TF signal status + lookback deltas
            indicators_by_tf: dict[str, dict] = {}
            for tf in TIMEFRAMES:
                ind = state.indicators.get(tf)
                if ind:
                    sig = self._tf_signals.get((sid, tf))
                    # Lookback delta for 10-100 bars (shows WHEN each spike occurred)
                    lb_delta = {
                        str(k): round(v, 2) if v is not None else None
                        for k, v in ind.lookback_delta.items()
                        if k <= 100
                    }
                    indicators_by_tf[tf] = {
                        "rsi":             round(ind.rsi, 1),
                        "rsi_ema":         round(ind.rsi_ema, 1),
                        "macd_hist":       round(ind.macd_hist, 4),
                        "bars":            len(state.bars.get(tf, [])),
                        "signal_status":   sig.status    if sig else None,
                        "signal_direction":sig.direction if sig else None,
                        # 10-bar lookback % — quick single-number spike indicator
                        "spk10":           round(ind.lookback_pct.get(10) or 0, 2),
                        # Full lookback delta table (10,20,...,100 bars)
                        "lb_delta":        lb_delta,
                    }

            active_sig = state.active_signals[-1] if state.active_signals else None

            opt_payload = {
                "symbol":           state.info.symbol,
                "strike":           state.info.strike,
                "option_type":      state.info.option_type,
                "label":            state.info.label,
                "ltp":              state.ltp,
                "ltp_change_pct":   round(state.ltp_change_pct, 2),
                "indicators":       indicators_by_tf,
                # Flat 1m values for backwards compatibility
                "rsi":       round((state.indicators.get("1m") or _empty_ind()).rsi, 1),
                "macd_hist": round((state.indicators.get("1m") or _empty_ind()).macd_hist, 4),
                "bars":      len(state.bars.get("1m", [])),
                "signal_status":    active_sig.status    if active_sig else None,
                "signal_direction": active_sig.direction if active_sig else None,
            }

            if underlying == "NIFTY":
                # Nifty index option → goes into nifty_data
                label_key = state.info.label.replace(" ", "_")  # "ATM_CE", "ITM_CE", etc.
                nifty_data["options"][label_key] = opt_payload
            else:
                # Stock option → goes into stocks_data
                opt_type = state.info.option_type   # "CE" or "PE"
                if underlying not in stocks_data:
                    stocks_data[underlying] = {
                        "spot": self._stock_spots.get(underlying, 0),
                        "options": {},
                    }
                else:
                    stocks_data[underlying]["spot"] = self._stock_spots.get(
                        underlying, stocks_data[underlying]["spot"]
                    )
                stocks_data[underlying]["options"][opt_type] = opt_payload

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
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "status":    self._status_msg,
            "active_tf": active_tf,
            "nifty":     nifty_data,
            "stocks":    stocks_data,
            "signals":   signals_data,
        }

    def _set_status(self, msg: str) -> None:
        self._status_msg = msg
        update_state({
            "status":    msg,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "active_tf": get_active_tf(),
            "nifty":     {"spot": 0, "options": {}},
            "stocks":    {},
            "signals":   [],
        })
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
