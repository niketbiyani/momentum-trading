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
    handlers=[
        logging.FileHandler("spike_detector.log"),
        logging.StreamHandler(),   # also print to terminal so errors are visible
    ],
)
logger = logging.getLogger(__name__)

# ── Project imports ────────────────────────────────────────────────────────────
from config import (
    DHAN_CLIENT_ID,
    DHAN_ACCESS_TOKEN,
    TIMEFRAMES,
    NIFTY_TIMEFRAMES,
    STOCK_TIMEFRAMES,
    NIFTY_SECURITY_ID,
    NIFTY50_STOCKS,
    BACKFILL_STOCK_OPTIONS,
    WEB_HOST,
    WEB_PORT,
)
from src.models import Bar, Tick, InstrumentState, OptionInfo, Signal
from src.bar_builder import MultiInstrumentBarBuilder
from src.bar_store import BarStore
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
        # DhanFeed returns LTP as a formatted string e.g. "23450.50"
        ltp = float(data.get("LTP", 0) or data.get("ltp", 0))
        if ltp <= 0:
            return None
        sec_id = str(data.get("security_id", data.get("Security Id", "")))
        if not sec_id:
            return None
        # LTT from DhanFeed.process_ticker() is already "HH:MM:SS" (not epoch).
        # Try epoch float first; fall back to datetime.now() for string formats.
        raw_ltt = data.get("LTT", data.get("ltt", 0)) or 0
        try:
            ltt_num = float(raw_ltt)
            if ltt_num > 1e12:
                ts = datetime.fromtimestamp(ltt_num / 1000)
            elif ltt_num > 0:
                ts = datetime.fromtimestamp(ltt_num)
            else:
                ts = datetime.now()
        except (ValueError, TypeError):
            ts = datetime.now()
        volume = int(data.get("volume", data.get("Volume", 0)) or 0)
        return Tick(timestamp=ts, security_id=sec_id, ltp=ltp, volume=volume)
    except Exception as e:
        logger.debug(f"Tick parse error: {e}")
        return None


def _on_message(data) -> None:
    if not data or not isinstance(data, dict):
        return
    tick = _parse_tick(data)
    if tick:
        try:
            _tick_queue.put_nowait(tick)
        except queue.Full:
            pass


# ── Feed startup ──────────────────────────────────────────────────────────────

def _start_dhan_feed(instruments: list[tuple]) -> threading.Thread:
    def _run():
        import asyncio
        # Python 3.10+ doesn't auto-create an event loop in background threads.
        # DhanFeed.__init__ calls asyncio.get_event_loop(), so we must create
        # one explicitly before constructing the feed.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            from dhanhq import marketfeed
        except ImportError:
            logger.error("dhanhq not installed. Run: pip install -r requirements.txt")
            sys.exit(1)

        backoff = 5  # seconds between reconnect attempts
        while True:
            try:
                feed = marketfeed.DhanFeed(
                    DHAN_CLIENT_ID,
                    DHAN_ACCESS_TOKEN,
                    instruments,
                    version='v2',
                )
                feed.run_forever()
                logger.info(
                    f"DhanFeed connected — {len(instruments)} instruments subscribed. "
                    "Starting data poll loop…"
                )
                backoff = 5  # reset on successful connect
                consecutive_errors = 0
                while True:
                    try:
                        data = feed.get_data()
                        _on_message(data)
                        consecutive_errors = 0
                    except Exception as tick_err:
                        consecutive_errors += 1
                        if consecutive_errors == 1:
                            logger.warning(f"DhanFeed get_data error: {tick_err}")
                        if consecutive_errors >= 10:
                            # Connection is dead — break inner loop to reconnect
                            logger.warning(
                                f"DhanFeed lost after {consecutive_errors} consecutive "
                                f"errors ({tick_err}) — reconnecting in {backoff}s…"
                            )
                            break
                        time.sleep(0.1)
            except Exception as e:
                logger.error(f"DhanFeed connect error: {e}")

            time.sleep(backoff)
            backoff = min(backoff * 2, 120)  # cap at 2 minutes

    t = threading.Thread(target=_run, name="dhan-feed", daemon=True)
    t.start()
    return t


# ── Historical backfill ───────────────────────────────────────────────────────

def _prior_trading_days(n: int) -> tuple[str, str]:
    """
    Return (from_date, to_date) spanning the last n trading days (Mon–Fri)
    ending today, as "YYYY-MM-DD" strings.
    E.g. n=2 on a Monday returns (last Friday, today).
    """
    from datetime import date, timedelta
    today = date.today()
    found = 0
    ref = today - timedelta(days=1)
    while found < n - 1:
        if ref.weekday() < 5:   # Mon=0 … Fri=4
            found += 1
        if found < n - 1:
            ref -= timedelta(days=1)
    return ref.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def _aggregate_bars(bars_1m: list, tf_seconds: int) -> list:
    """
    Aggregate a list of 1m Bar objects into larger timeframe bars.
    Each output bar covers one tf_seconds bucket aligned to epoch boundaries
    (same flooring used by BarBuilder._get_bar_start).
    """
    from collections import defaultdict
    buckets: dict = defaultdict(list)
    for bar in bars_1m:
        epoch = int(bar.timestamp.timestamp())
        bucket_start = (epoch // tf_seconds) * tf_seconds
        buckets[bucket_start].append(bar)

    result = []
    for bucket_start in sorted(buckets.keys()):
        group = buckets[bucket_start]
        result.append(Bar(
            timestamp=datetime.fromtimestamp(bucket_start),
            open=group[0].open,
            high=max(b.high for b in group),
            low=min(b.low for b in group),
            close=group[-1].close,
            volume=sum(b.volume for b in group),
        ))
    return result


def _backfill_1m(dhan_client, security_id: str, exchange_segment: str,
                 instrument_type: str,
                 bar_builder: MultiInstrumentBarBuilder,
                 n_days: int = 5) -> None:
    """
    Load n_days of 1-minute OHLC history for one instrument so that RSI,
    MACD and the full 150-bar lookback table are populated from startup.

    n_days=5  →  Dhan's intraday_minute_data supports up to 5 trading days.
    The 1m bars are stored in the bar_builder; 3m bars are derived by
    aggregation so the 3m indicator engine is also pre-seeded without a
    separate API call.
    """
    try:
        from_date, to_date = _prior_trading_days(n_days)

        resp = dhan_client.intraday_minute_data(
            security_id=security_id,
            exchange_segment=exchange_segment,
            instrument_type=instrument_type,
            from_date=from_date,
            to_date=to_date,
        )

        # ── 1. Check API-level status ─────────────────────────────────────
        status = resp.get("status")
        if status != "success":
            logger.warning(
                f"Backfill API failure for {security_id} "
                f"({exchange_segment}/{instrument_type} {from_date}→{to_date}): "
                f"status={status!r} remarks={resp.get('remarks')} "
                f"raw={str(resp.get('data', ''))[:300]}"
            )
            return

        # ── 2. Navigate response dict (handle flat vs nested shapes) ──────
        raw = resp.get("data", {})

        # Some Dhan endpoints wrap in an extra "data" key, e.g.
        # {"data": {"open": [...], ...}} — unwrap if present and "open" missing
        if isinstance(raw, dict) and not raw.get("open") and isinstance(raw.get("data"), dict):
            raw = raw["data"]

        if not isinstance(raw, dict):
            logger.warning(
                f"Unexpected response type for {security_id}: "
                f"type={type(raw).__name__} raw={str(raw)[:300]}"
            )
            return

        opens   = raw.get("open",  [])
        highs   = raw.get("high",  [])
        lows    = raw.get("low",   [])
        closes  = raw.get("close", [])
        volumes = raw.get("volume", [])
        times   = raw.get("start_Time", raw.get("startTime", raw.get("timestamp", [])))

        if not closes:
            logger.warning(
                f"No close data for {security_id} ({from_date}→{to_date}). "
                f"Response keys: {list(raw.keys())}"
            )
            return

        bars: list[Bar] = []
        for i in range(len(closes)):
            try:
                c = float(closes[i])
            except (TypeError, ValueError):
                continue
            if c <= 0:
                continue
            if times:
                ts_val = float(times[i])
                # REST API returns seconds; guard against milliseconds (> year 2286)
                ts = datetime.fromtimestamp(ts_val / 1000 if ts_val > 1e10 else ts_val)
            else:
                ts = datetime.now()
            bars.append(Bar(
                timestamp=ts,
                open=float(opens[i]) if opens else c,
                high=float(highs[i]) if highs else c,
                low=float(lows[i]) if lows else c,
                close=c,
                volume=int(float(volumes[i])) if volumes else 0,
            ))

        if bars:
            bar_builder.add_historical_bars(security_id, "1m", bars)
            bars_3m = _aggregate_bars(bars, 180)
            if bars_3m:
                bar_builder.add_historical_bars(security_id, "3m", bars_3m)
            logger.info(
                f"Backfilled {len(bars)} 1m + {len(bars_3m)} 3m bars "
                f"for {security_id} ({from_date} → {to_date})"
            )
        else:
            logger.warning(
                f"No valid bars for {security_id} ({from_date}→{to_date}): "
                f"{len(closes)} closes received but all were 0/invalid"
            )
    except Exception as e:
        logger.warning(f"Backfill failed for {security_id}: {e}", exc_info=True)


# ── Main app ──────────────────────────────────────────────────────────────────

class SpikeDetectorApp:
    """Wires all components together and runs the processing loop."""

    def __init__(self):
        self._stock_manager  = StockOptionsManager()
        self._nifty_manager  = OptionsManager()
        self._bar_builder    = MultiInstrumentBarBuilder(on_bar_close=self._on_bar_close)
        self._bar_store      = BarStore()    # persists 5s/15s bars across restarts
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

        # 2. Init Dhan REST client first (needed for fetch_security_list fallback)
        self._set_status("Connecting to Dhan API…")
        try:
            from dhanhq import dhanhq
            self._dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
            logger.info(
                f"Dhan client ready — client_id={DHAN_CLIENT_ID[:4]}… "
                f"token={DHAN_ACCESS_TOKEN[:8]}…"
            )
        except ImportError:
            print("[ERROR] dhanhq not installed. Run: pip install -r requirements.txt")
            sys.exit(1)
        except Exception as e:
            print(f"[ERROR] Dhan client init failed: {e}")
            sys.exit(1)

        # 3. Load instruments master (try disk cache/download; fall back to
        #    dhanhq.fetch_security_list() if the URL fetch fails)
        self._set_status("Loading instruments master…")
        if not self._stock_manager.initialise():
            logger.warning("Instruments master (stock) URL load failed — trying fetch_security_list()")
            self._stock_manager.initialise_from_dhan(self._dhan)
        if not self._nifty_manager.initialise():
            logger.warning("Instruments master (nifty) URL load failed — trying fetch_security_list()")
            self._nifty_manager.initialise_from_dhan(self._dhan)

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
        # Diagnostic: show exactly what security IDs were resolved.
        # If any show "(unresolved)" the instruments master lookup failed for that strike.
        # If isdigit=False the option will be silently skipped in the feed subscription.
        for _info in nifty_instruments:
            logger.info(
                f"  {_info.label}: security_id={_info.security_id!r}  "
                f"symbol={_info.symbol}  isdigit={_info.security_id.isdigit()}"
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

        # 8.5 Load persisted 5s/15s bars (saved from previous session).
        #     These are bars the Dhan API can't provide (sub-minute resolution).
        #     We load them before the 1m backfill so the bar deques are populated
        #     and the indicator engines can be seeded with real sub-minute history.
        self._set_status("Loading cached 5s/15s bars from previous session…")
        pruned = self._bar_store.prune_old()
        if pruned:
            logger.info(f"Bar store: pruned {pruned} stale rows")
        loaded_bars = 0
        for sid in list(self._instrument_states.keys()):
            for tf in ("5s", "15s"):
                bars = self._bar_store.load_bars(sid, tf)
                if bars:
                    self._bar_builder.add_historical_bars(sid, tf, bars)
                    loaded_bars += len(bars)
        if loaded_bars:
            logger.info(f"Bar store: loaded {loaded_bars} persisted sub-minute bars")
        else:
            logger.info("Bar store: no persisted bars found (fresh start)")

        # 9. Backfill 1m bars so RSI/MACD have real history from startup.
        #    Nifty index options are always backfilled (only 4 instruments).
        #    Stock options are gated by BACKFILL_STOCK_OPTIONS env var (100 API calls).
        self._set_status("Backfilling Nifty option bars…")
        for info in nifty_instruments:
            if info.security_id.isdigit():
                _backfill_1m(self._dhan, info.security_id, "NSE_FNO",
                             "OPTIDX", self._bar_builder)

        if BACKFILL_STOCK_OPTIONS:
            self._set_status("Backfilling stock option bars…")
            for info in stock_instruments:
                if info.security_id.isdigit():
                    _backfill_1m(self._dhan, info.security_id, "NSE_FNO",
                                 "OPTSTK", self._bar_builder)

        # 9b. Seed indicator engines from backfilled bars.
        #     add_historical_bars() only fills the BarBuilder deque — it doesn't
        #     push closes into IndicatorEngine.  We do that here so RSI/MACD/
        #     lookback values are available immediately (not only after the first
        #     bar closes from a live tick).
        seeded = 0
        for sid, state in self._instrument_states.items():
            for tf in TIMEFRAMES:
                closes = self._bar_builder.get_closes(sid, tf, include_current=False)
                if len(closes) >= 2:
                    engine = self._indicator_engines.get((sid, tf))
                    if engine:
                        engine.load_closes(closes)
                        ind = engine.compute()
                        state.indicators[tf] = ind
                        state.bars[tf] = self._bar_builder.get_bars(sid, tf)
                        seeded += 1
        if seeded:
            logger.info(f"Seeded {seeded} indicator engines from backfilled history")

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
        nifty_opt_subs = [i for i in nifty_instruments if i.security_id.isdigit()]
        if len(nifty_opt_subs) < len(nifty_instruments):
            logger.warning(
                f"Only {len(nifty_opt_subs)}/{len(nifty_instruments)} Nifty options "
                "have valid security IDs — the rest will NOT receive live ticks. "
                "Check the instruments master log lines above for details."
            )
        self._set_status("Connecting to Dhan market feed…")
        _start_dhan_feed(feed_instruments)
        logger.info(f"Feed started — {len(feed_instruments)} subscriptions "
                    f"({len(nifty_opt_subs)} Nifty options, "
                    f"{len([i for i in stock_instruments if i.security_id.isdigit()])} stock options)")

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

        # Persist 5s/15s bars so they survive restarts (1m comes from the API)
        self._bar_store.write_bar(security_id, timeframe, bar)

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
                        if engine:
                            # include_current=False: only completed bars go into the
                            # engine so that _on_bar_close's push_close() adds exactly
                            # one new close without duplicating the current bar.
                            closes = self._bar_builder.get_closes(
                                tick.security_id, tf, include_current=False
                            )
                            if closes:
                                engine.load_closes(closes)
                                # Keep dashboard indicators live on every tick so
                                # the heatmap (5s/15s/1m) always reflects current bars.
                                if state:
                                    state.indicators[tf] = engine.compute()

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
                    # Lookback pct: current price vs N bars ago (10,20,...,100)
                    lb_pct = {
                        str(k): round(v, 2) if v is not None else None
                        for k, v in ind.lookback_pct.items()
                        if k <= 100
                    }
                    # Lookback delta: move WITHIN each 10-bar window
                    # delta[10] = move in last 10 bars
                    # delta[20] = move in bars 11-20 (where was the spike?)
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
                        # Cumulative % move: current price vs N bars ago
                        "lb_pct":          lb_pct,
                        # Delta % move: move WITHIN each 10-bar window (for pinpointing spike)
                        "lb_delta":        lb_delta,
                        # Z-score: (current 10b move - mean baseline) / std baseline
                        "spike_zscore":    ind.spike_zscore,
                    }

            active_sig = state.active_signals[-1] if state.active_signals else None

            opt_payload = {
                "symbol":           state.info.symbol,
                "strike":           state.info.strike,
                "option_type":      state.info.option_type,
                "label":            state.info.label,
                "ltp":              state.ltp,
                "ltp_change_pct":   round(state.ltp_change_pct or 0, 2),
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
