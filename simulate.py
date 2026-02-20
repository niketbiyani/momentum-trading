#!/usr/bin/env python3
"""
TradingView 1-second CSV replay simulator.

Replays a downloaded 1s OHLCV CSV through the exact same pipeline
(bar builder → indicator engines → signal engine) and serves the live
dashboard — no Dhan credentials or live feed required.

Usage:
    python simulate.py data.csv
    python simulate.py data.csv --label "ATM CE" --speed 50 --port 8000

TradingView CSV format (1s bars):
    time,open,high,low,close,volume
    1708944000,450.25,451.10,449.80,450.90,1234

Speed examples:
    --speed 1    real-time (default)
    --speed 50   50× faster than real time
    --speed 0    as fast as possible (state pushed every 500ms wall-clock)
"""
import argparse
import csv
import logging
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from config import (
    TIMEFRAMES,
    TF_10MIN_BARS,
    RSI_MIN_BARS,
    RSI_EMA_MIN_BARS,
    MACD_MIN_BARS,
)
from src.models import OptionInfo, InstrumentState, Tick, Signal, Indicators
from src.bar_builder import MultiInstrumentBarBuilder
from src.indicators import IndicatorEngine
from src.spike_detector import SignalEngine
from web.server import start_server, update_state, get_active_tf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulate")

SIM_ID = "SIM_OPTION"


# ── CSV parsing ────────────────────────────────────────────────────────────────

def _parse_ts(raw: str) -> datetime:
    """
    Parse a TradingView timestamp. Handles:
      - Unix seconds   (e.g. 1708944000)
      - Unix milliseconds (e.g. 1708944000000)
      - ISO date string (e.g. "2024-02-26 09:15:00")
    """
    raw = raw.strip()
    try:
        ts = float(raw)
        if ts > 1e12:
            ts /= 1000          # ms → s
        return datetime.fromtimestamp(ts)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse timestamp: {raw!r}")


def load_csv(path: str) -> list[tuple[datetime, float, float, float, float, int]]:
    """
    Load a TradingView 1s CSV. Accepts any column ordering and is
    case-insensitive. Returns rows sorted by timestamp.
    """
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header row")
        # Normalise header names
        reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]

        ts_col = next(
            (c for c in reader.fieldnames
             if c in ("time", "date", "datetime", "timestamp")),
            None,
        )
        if ts_col is None:
            raise ValueError(
                f"No timestamp column found. Got: {reader.fieldnames}"
            )

        for row in reader:
            try:
                ts  = _parse_ts(row[ts_col])
                o   = float(row["open"])
                h   = float(row["high"])
                l   = float(row["low"])
                c   = float(row["close"])
                vol = int(float(row.get("volume", "0") or 0))
                rows.append((ts, o, h, l, c, vol))
            except (KeyError, ValueError) as e:
                log.warning(f"Skipping row — {e}")

    if not rows:
        raise ValueError("CSV contained no valid rows")

    rows.sort(key=lambda r: r[0])
    log.info(
        f"Loaded {len(rows):,} rows from {Path(path).name!r}  "
        f"({rows[0][0].strftime('%H:%M:%S')} → {rows[-1][0].strftime('%H:%M:%S')})"
    )
    return rows


# ── State builder ──────────────────────────────────────────────────────────────

def _empty_ind() -> Indicators:
    return Indicators()


def _build_web_state(
    state:      InstrumentState,
    tf_signals: dict,
    all_signals: deque,
    bar_builder: MultiInstrumentBarBuilder,
    row_idx:    int,
    total_rows: int,
    sim_ts:     datetime,
) -> dict:
    """
    Build the same state dict that main.py pushes to the browser.
    The option goes into nifty.options so it shows up in the Nifty tab.
    """
    active_tf = get_active_tf()

    indicators_by_tf: dict = {}
    for tf in TIMEFRAMES:
        ind = state.indicators.get(tf)
        if not ind:
            continue

        sig    = tf_signals.get((SIM_ID, tf))
        n_bars = len(state.bars.get(tf, []))
        tf_10m = TF_10MIN_BARS.get(tf, 10)

        lb_pct = {
            str(k): round(v, 2) if v is not None else None
            for k, v in ind.lookback_pct.items()
            if k <= 100
        }
        lb_delta = {
            str(k): round(v, 2) if v is not None else None
            for k, v in ind.lookback_delta.items()
            if k <= 100
        }

        indicators_by_tf[tf] = {
            "rsi":       round(ind.rsi, 1)       if n_bars >= RSI_MIN_BARS      else None,
            "rsi_ema":   round(ind.rsi_ema, 1)   if n_bars >= RSI_EMA_MIN_BARS  else None,
            "macd_hist": round(ind.macd_hist, 4) if n_bars >= MACD_MIN_BARS     else None,
            "bars":             n_bars,
            "signal_status":    sig.status    if sig else None,
            "signal_direction": sig.direction if sig else None,
            "spk_10m": (
                round(ind.lookback_pct.get(tf_10m) or 0, 2)
                if n_bars > tf_10m else None
            ),
            "lb_pct":      lb_pct,
            "lb_delta":    lb_delta,
            "spike_zscore":ind.spike_zscore,
        }

    active_sig = state.active_signals[-1] if state.active_signals else None
    ind_1m     = state.indicators.get("1m") or _empty_ind()
    n_1m       = len(state.bars.get("1m", []))

    label_key = state.info.label.replace(" ", "_")   # "ATM CE" → "ATM_CE"

    opt_payload = {
        "symbol":           state.info.symbol,
        "strike":           state.info.strike,
        "option_type":      state.info.option_type,
        "label":            state.info.label,
        "ltp":              state.ltp,
        "ltp_change_pct":   round(state.ltp_change_pct or 0, 2),
        "indicators":       indicators_by_tf,
        # Flat 1m values (backwards-compat with dashboard JS)
        "rsi":       round(ind_1m.rsi, 1),
        "macd_hist": round(ind_1m.macd_hist, 4),
        "bars":      n_1m,
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
        for s in list(all_signals)[:50]
    ]

    return {
        "timestamp": sim_ts.strftime("%H:%M:%S"),
        "status":    (
            f"Simulating…  {row_idx:,} / {total_rows:,}"
            f"  ({sim_ts.strftime('%H:%M:%S')})"
        ),
        "active_tf": active_tf,
        "nifty": {
            "spot": 0,
            "options": {label_key: opt_payload},
        },
        "stocks":  {},
        "signals": signals_data,
    }


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay a TradingView 1s CSV through the spike detector."
    )
    parser.add_argument("csv",
                        help="Path to TradingView 1s OHLCV CSV file")
    parser.add_argument("--label",       default="SIM CE",
                        help='Display label shown on the dashboard (default: "SIM CE")')
    parser.add_argument("--option-type", default="CE",
                        help='"CE" or "PE" (default: CE)')
    parser.add_argument("--strike",      type=int, default=0,
                        help="Strike price for display only (default: 0)")
    parser.add_argument("--speed",       type=float, default=1.0,
                        help="Playback speed multiplier (default 1.0; 0 = max speed)")
    parser.add_argument("--port",        type=int, default=8000,
                        help="Web server port (default: 8000)")
    args = parser.parse_args()

    # ── Load data ─────────────────────────────────────────────────────────────
    rows  = load_csv(args.csv)
    total = len(rows)

    # Estimate median row interval for pacing
    if total > 1:
        sample = [
            (rows[i + 1][0] - rows[i][0]).total_seconds()
            for i in range(min(200, total - 1))
        ]
        sample = [d for d in sample if 0 < d <= 60]
        row_interval = sorted(sample)[len(sample) // 2] if sample else 1.0
    else:
        row_interval = 1.0

    sleep_per_row = row_interval / args.speed if args.speed > 0 else 0.0
    eta_min = total * row_interval / max(args.speed, 0.001) / 60

    log.info(
        f"Row interval: {row_interval:.1f}s  |  Speed: {args.speed}×  |  "
        f"ETA: {eta_min:.1f} min"
    )

    # ── Instrument + state ────────────────────────────────────────────────────
    info = OptionInfo(
        security_id=SIM_ID,
        symbol=f"SIM_{args.label.replace(' ', '_')}",
        strike=args.strike,
        option_type=args.option_type.upper(),
        expiry="",
        label=args.label,
        underlying="NIFTY",   # routes opt_payload into the Nifty tab
    )
    state = InstrumentState(info=info)

    # ── Pipeline ──────────────────────────────────────────────────────────────
    tf_signals:  dict[tuple[str, str], Signal] = {}
    all_signals: deque[Signal] = deque(maxlen=100)

    indicator_engines: dict[tuple[str, str], IndicatorEngine] = {
        (SIM_ID, tf): IndicatorEngine()
        for tf in TIMEFRAMES
    }

    signal_engine = SignalEngine()
    for tf in TIMEFRAMES:
        signal_engine.register(info, tf)

    def _on_bar_close(security_id: str, tf: str, bar) -> None:
        eng = indicator_engines[(security_id, tf)]
        eng.push_close(bar.close)
        indicators = eng.compute()

        state.indicators[tf] = indicators
        state.bars[tf]       = bar_builder.get_bars(security_id, tf)

        sig = signal_engine.evaluate(
            security_id=security_id,
            timeframe=tf,
            indicators=indicators,
            bar_high=bar.high,
            bar_low=bar.low,
            bar_close=bar.close,
        )
        if sig:
            sig.underlying = info.underlying
            state.active_signals = [sig]
            tf_signals[(security_id, tf)] = sig
            all_signals.appendleft(sig)
            log.info(f"  ★ SIGNAL  {sig}")

    bar_builder = MultiInstrumentBarBuilder(on_bar_close=_on_bar_close)
    bar_builder.register(SIM_ID)

    # ── Web server ────────────────────────────────────────────────────────────
    start_server(port=args.port)
    log.info(f"Dashboard → http://localhost:{args.port}   (Ctrl+C to quit)")
    time.sleep(0.6)   # let uvicorn bind before we start pushing state

    # ── Replay ────────────────────────────────────────────────────────────────
    last_push = time.time()

    for i, (ts, o, h, l, c, vol) in enumerate(rows, 1):
        tick = Tick(timestamp=ts, security_id=SIM_ID, ltp=c, volume=vol)

        state.prev_ltp    = state.ltp
        state.ltp         = c
        state.last_update = ts

        bar_builder.on_tick(tick)   # may fire _on_bar_close

        # Keep indicators live on every tick (completed bars only — mirrors main.py)
        for tf in TIMEFRAMES:
            closes = bar_builder.get_closes(SIM_ID, tf, include_current=False)
            if closes:
                eng = indicator_engines[(SIM_ID, tf)]
                eng.load_closes(closes)
                state.indicators[tf] = eng.compute()

        # Push to browser every 500ms wall-clock
        now = time.time()
        if now - last_push >= 0.5 or (sleep_per_row == 0 and i % 500 == 0):
            update_state(_build_web_state(
                state, tf_signals, all_signals, bar_builder, i, total, ts,
            ))
            last_push = now

        if sleep_per_row > 0:
            time.sleep(sleep_per_row)

    # Final push
    update_state(_build_web_state(
        state, tf_signals, all_signals, bar_builder, total, total, rows[-1][0],
    ))
    log.info(f"Replay complete — {len(all_signals)} signal(s) fired.")
    log.info("Dashboard remains live. Ctrl+C to quit.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
