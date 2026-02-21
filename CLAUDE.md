# Nifty Options Spike Detector — Project Context

> **Purpose of this file:** Full project context so any new Claude session can
> immediately understand the codebase, current state, and pending work without
> needing to re-explore everything from scratch.

---

## 1. What This Project Does

A **real-time options spike detection dashboard** for Indian markets (Nifty 50
index options and individual stock options). It connects to the Dhan API, streams
live price ticks, runs a multi-timeframe technical analysis pipeline, and serves a
browser dashboard at `http://localhost:8000`.

**The core idea (user's strategy):**
1. An options price spikes sharply in a short window (10 bars).
2. Confirm: spike is statistically abnormal (≥2σ above 100-bar historical mean).
3. Confirm: RSI hit overbought/oversold within the spike window.
4. Confirm: current RSI stays above 45 (UP) or below 55 (DOWN), and MACD confirms.
5. Wait for a small consolidation (≥3 tight bars), then enter on the breakout.

**Also has a CSV replay simulator** (`simulate.py`) that replays a TradingView
1-second OHLCV CSV through the exact same pipeline, so the strategy can be tested
on historical data without live credentials.

---

## 2. Repository Layout

```
momentum-trading/
├── main.py                  # Live trading entry point (Dhan API)
├── simulate.py              # Historical replay entry point (TradingView CSV)
├── config.py                # All configurable constants (env-overridable)
├── requirements.txt         # Python dependencies
├── .env.example             # Template for credentials
│
├── src/
│   ├── models.py            # Core dataclasses: Bar, Tick, Indicators, Signal, etc.
│   ├── bar_builder.py       # Converts tick stream → OHLCV bars (multi-TF)
│   ├── indicators.py        # RSI, MACD, lookback%, z-score calculations
│   ├── spike_detector.py    # Signal state machine (SPIKE→WATCH→ENTRY→EXPIRED)
│   ├── options_manager.py   # Dhan API: discover + subscribe to ATM options
│   ├── stock_manager.py     # Nifty 50 stock options subscription
│   ├── bar_store.py         # Persistent bar storage helper
│   └── dashboard.py        # (Legacy rich terminal dashboard, unused)
│
└── web/
    ├── server.py            # FastAPI + WebSocket server (500ms broadcast loop)
    └── static/
        ├── index.html       # Single-page dashboard
        ├── style.css        # Dark-theme CSS (GitHub dark palette)
        └── app.js           # All frontend logic (WebSocket, render, sigma chart)
```

---

## 3. How to Run

### A. Simulator (no credentials needed)

```bash
python simulate.py data.csv
python simulate.py data.csv --label "ATM CE" --speed 50 --port 8000
```

- `--speed 1` = real-time (default)
- `--speed 50` = 50× faster
- `--speed 0` = max speed, push every 500ms wall-clock
- Dashboard at `http://localhost:8000`

**CSV format** (TradingView 1s export):
```
time,open,high,low,close,volume
1708944000,450.25,451.10,449.80,450.90,1234
```
Timestamp can be Unix seconds, Unix milliseconds, or `YYYY-MM-DD HH:MM:SS`.

### B. Live (Dhan API)

```bash
cp .env.example .env
# Fill in DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN
python main.py
```

### C. Environment Variables (all optional, have defaults in config.py)

| Variable | Default | Purpose |
|---|---|---|
| `DHAN_CLIENT_ID` | — | Dhan API client ID |
| `DHAN_ACCESS_TOKEN` | — | Dhan API token |
| `WEB_HOST` | `0.0.0.0` | Web server bind host |
| `WEB_PORT` | `8000` | Web server port |
| `SPIKE_ZSCORE_MIN` | `2.0` | Min σ to qualify as spike |
| `SPIKE_THRESHOLD_PCT` | `1.0` | Min absolute % move (hard floor) |
| `RSI_OVERBOUGHT` | `70` | RSI peak threshold within spike window |
| `RSI_OVERSOLD` | `30` | RSI trough threshold within spike window |
| `RSI_UP_MIN` | `45` | RSI must stay above this after UP spike |
| `RSI_DOWN_MAX` | `55` | RSI must stay below this after DOWN spike |
| `BACKFILL_STOCK_OPTIONS` | `false` | Backfill all 50 stock options on startup |

---

## 4. Architecture: Data Flow

```
[Dhan WebSocket tick feed]       [TradingView CSV]
         │                              │
         ▼                              ▼
    main.py                       simulate.py
         │                              │
         └──────────┬───────────────────┘
                    │  Tick(ts, security_id, ltp, volume)
                    ▼
         MultiInstrumentBarBuilder      ← bar_builder.py
         (groups ticks into 5s/15s/1m/3m OHLCV bars)
                    │
                    │ on_bar_close(security_id, tf, bar)
                    ▼
         IndicatorEngine.compute()      ← indicators.py
         (RSI-14, EMA-50-of-RSI, MACD-12/26/9,
          lookback%, lookback_delta, spike_zscore)
                    │
                    ▼
         SignalEngine → SpikeDetector   ← spike_detector.py
         (z-score ≥ 2σ + RSI conf → SPIKE → WATCH → ENTRY)
                    │
                    ▼
         update_state(web_state_dict)   ← web/server.py
                    │
         FastAPI _broadcast_task        (every 500ms)
                    │  WebSocket JSON push
                    ▼
         Browser (app.js render loop)
```

---

## 5. Key Algorithms

### Spike Z-Score (`calc_spike_zscore` in `indicators.py`)

```
z = (|current_10bar_move| - mean_baseline) / std_baseline
```

- **current_10bar_move**: `|closes[-1] - closes[-11]|` / `closes[-11]` × 100
- **baseline**: 100 rolling 10-bar moves from bars 11–120 ago
- Requires minimum 120 bars. Returns `None` during warmup.
- Also computes `spike_zscore_d20` (the move 11-20 bars ago vs the same baseline).

### Signal State Machine (`spike_detector.py`)

```
None → SPIKE  (z ≥ 2σ + RSI hit OB/OS in spike window)
SPIKE → WATCH (RSI ≥ 45 for UP / ≤ 55 for DOWN, MACD confirms)
WATCH → ENTRY (≥3 tight consolidation bars, then breakout)
Any → EXPIRED (RSI/MACD conditions broken)
```

### Lookback Table (`calc_lookback_pct`)

For each N in `[10, 20, 30, 40, ..., 150]`:
```
pct[N] = (current_close - close_N_bars_ago) / close_N_bars_ago * 100
```

`delta[N] = pct[N] - pct[N-10]` — the move concentrated in the 10-bar window
ending N bars ago. A large `|delta[10]|` = big move just happened.

---

## 6. Web Dashboard (app.js / index.html)

### Layout
```
┌─────────────────────────────────────────────┐
│  Header: status dot, timestamp, sim controls│
├─────────────────────────────────────────────┤
│  Sigma chart strip (spike z-score over time)│
├─────────────────────────────────────────────┤
│  [Nifty tab] [Stocks tab]                   │
│                                             │
│  Options table: Symbol | LTP | RSI | MACD  │
│                 Spk% | 5s | 15s | 1m cols  │
├─────────────────────────────────────────────┤
│  Lookback heatmap (pct or delta mode)       │
├─────────────────────────────────────────────┤
│  Signals log                                │
└─────────────────────────────────────────────┘
```

### Sigma Chart Strip (`_appendSigma` / `_drawSigma` in `app.js`)

A Canvas-based rolling chart showing `spike_zscore` over time for all three
timeframes simultaneously:

- **Green** (`#3fb950`): 5s timeframe
- **Orange** (`#f0883e`): 15s timeframe
- **Blue** (`#58a6ff`): 1m timeframe
- Y-axis: 0–8σ by default (auto-expands). Reference lines at each integer σ.
  - ≥5σ = red (extreme), ≥3σ = orange (notable), <3σ = dim white
- 600-point rolling history. Right-aligned (newest = right).
- Chart height: 140px. Left pad: 36px (wide enough for "7σ" label).
- Only starts recording once the **first real σ value** appears (skips warmup nulls).
- Appending is gated by `sim_idx` change + `sim_paused` — no duplicate points
  while paused.

### Sim Controls (simulator mode only, hidden in live mode)

- **Play/Pause**: `sim_pause` → server sets `_sim_control["paused"]`
- **Step**: advances one tick at a time
- **Back step**: rewinds N ticks by replaying from row 0 silently
- **Speed slider**: 0.1× to 50×, sends `sim_speed` to server

### WebSocket Protocol

**Server → Browser** (every 500ms, JSON):
```json
{
  "timestamp": "09:30:15",
  "sim_ts": "09:30:15",
  "sim_idx": 1234,
  "sim_total": 22500,
  "sim_paused": false,
  "sim_speed": 1.0,
  "active_tf": "1m",
  "nifty": {
    "spot": 22150,
    "options": {
      "ATM_CE": {
        "ltp": 245.5,
        "indicators": {
          "5s":  { "spike_zscore": 2.3, "rsi": 72.1, ... },
          "15s": { "spike_zscore": 1.8, "rsi": 68.4, ... },
          "1m":  { "spike_zscore": 3.1, "rsi": 75.2, ... }
        }
      }
    }
  },
  "signals": [ ... ]
}
```

**Browser → Server** (on user action):
```json
{ "set_tf": "15s" }           // change active timeframe
{ "sim_pause": true }         // pause/resume
{ "sim_speed": 50 }           // change speed
{ "sim_step": 1 }             // advance one tick
{ "sim_back": 100 }           // rewind 100 ticks
```

---

## 7. Current Git Branch

Branch: `claude/options-spike-detection-Y18qF`

All development happens on this branch. Push with:
```bash
git push -u origin claude/options-spike-detection-Y18qF
```

---

## 8. What Has Been Built (Commit History Summary)

| Commit | What |
|---|---|
| `8e36da1` | Sigma chart overhaul: height, colours, y-range, timestamp visibility |
| `ff8c158` | Fix: pause ticker + warmup null suppression at 50x |
| `b6027a2` | Fix sigma canvas sizing |
| `25283fa` | Fix sigma chart not rendering (timing + opacity) |
| `d234a60` | **Add Spkσ time-series chart strip** (sigma chart, major feature) |
| `baf1f05` | Back step button for sim |
| `94d8ad4` | Browser playback controls (play/pause/step/speed) |
| `c49b844` | TradingView CSV replay simulator |
| `1068fac` | Remove tick price chart |
| `b029a20` | Cache-busting for app.js |
| `f4a4d20` | Replace fixed threshold with z-score spike detection |
| `95f3eaf` | RSI overbought confirmation + z-score clarity |
| `c7e6d49` | 5s/15s bar persistence, heatmap delta toggle, strategy legend |
| `ae556dc` | Lookback heatmap cumulative pct vs delta toggle |
| `06be9b4` | Initial .gitignore |

---

## 9. Known Issues / Pending Work (as of 2026-02-21)

### Recently fixed
- Sigma chart ticker continuous even when paused ✓ (two guards: `sim_idx` +
  `sim_paused`)
- Warmup nulls filling sigma chart buffer at high speeds ✓ (don't record until
  first real σ)
- Sigma chart colours too similar ✓ (green/orange/blue)
- Chart too short, y-axis too narrow ✓ (140px, pad=36, y-floor=8σ)
- Timestamp invisible ✓ (bright colour, bottom anchor)

### User is investigating / may want next
- 50x speed: verify calculations are actually running correctly end-to-end
  (suspect was warmup issue now fixed)
- Possible: add keyboard shortcuts (space = pause, arrow = step)
- Possible: add σ value labels on the chart at the rightmost visible point
  for each TF (so you can read current value without hovering)
- Possible: `spike_zscore_d20` (the 11-20 bar window z-score) visualised
  separately or overlaid on the same chart
- Live mode (`main.py`) has not been tested recently — simulator is the
  primary development vehicle

### Architecture debt
- The sigma chart x-axis has no wall-clock time markers (just sim_ts at
  the right edge). A proper time ruler would help orientation.
- `bar_store.py` exists but is not wired into either `main.py` or
  `simulate.py` — bars are kept in-memory only.
- `dashboard.py` (rich terminal) is a dead file — not called anywhere.
- Back-step replay in `simulate.py` (lines 338-394) processes N rows
  silently, which is O(N) and slow for large rewinds. A pre-indexed
  checkpoint approach would be faster.

---

## 10. Python Pipeline Details

### IndicatorEngine (`src/indicators.py`)

Stateful per `(instrument, timeframe)` pair. Holds a rolling `deque` of close
prices (max `MAX_BARS = 400`).

Key methods:
- `push_close(close)` — append one bar's close
- `load_closes(closes)` — replace buffer (used to sync from bar builder)
- `compute()` → `Indicators` — runs all calculations

**Warmup requirements** (before these return real values):
| Indicator | Bars needed |
|---|---|
| RSI-14 | 15 bars |
| RSI EMA-50 | 64 bars |
| MACD (12,26,9) | 35 bars |
| `spike_zscore` | **120 bars** (2×10 + 100) |

### BarBuilder (`src/bar_builder.py`)

Converts ticks → OHLCV bars for timeframes `{5s: 5, 15s: 15, 1m: 60, 3m: 180}`.

Bars are floor-aligned to Unix epoch (e.g., a 5s bar starting at 09:15:00 covers
09:15:00–09:15:05). The `on_bar_close` callback fires when a new bar starts
(i.e., when the previous bar just closed).

### SpikeDetector (`src/spike_detector.py`)

One instance per `(instrument, timeframe)`. State: `None | SpikeState`.

`_find_spike()` checks `delta[10]` and `delta[20]` z-scores. If best z ≥ 2σ
AND absolute move ≥ 1% AND RSI hit OB/OS in that window → spike found.

Consolidation tracking: after WATCH, tracks whether successive bars are "tight"
(`(H-L)/C < 0.8%`). After 3+ tight bars followed by a wide bar that breaks the
high/low of the consolidation zone → ENTRY signal.

---

## 11. Frontend Code Map (app.js ~640 lines)

| Lines | Function | Purpose |
|---|---|---|
| 1–42 | `connect()` | WebSocket setup, reconnect logic |
| 60–66 | `setInterval(updateClock)` | Live IST clock |
| 68–106 | `switchTab`, `sendTf`, search | Tab + TF + search controls |
| 200–210 | `render()` | Dispatcher: calls all sub-renderers on every WS message |
| 210–350 | `renderStatus`, `renderNiftyTab` | Status bar + options table |
| 350–462 | `renderLookbackHeatmap` | Colour-coded lookback % / delta table |
| 462–512 | `renderSimControls` | Play/pause/step/speed controls |
| 506–534 | `_appendSigma` | Append to sigma history ring buffer |
| 534–640 | `_drawSigma` | Canvas drawing: axes, ref lines, traces, timestamp |

**Critical sigma chart constants:**
```javascript
const SIGMA_MAX_PTS = 600;           // ring buffer size
const SIGMA_COLORS  = { '5s': '#3fb950', '15s': '#f0883e', '1m': '#58a6ff' };
let   lastSimIdx    = -1;            // guard against duplicate appends
```

---

## 12. How to Continue This Work in a New Chat

1. **Open project**: working directory is `/home/user/momentum-trading`
2. **Check out branch**: `git checkout claude/options-spike-detection-Y18qF`
3. **Read this file**: everything you need is here
4. **Key files to read** if you need code details:
   - `web/static/app.js` lines 506–640 (sigma chart)
   - `simulate.py` (full sim loop, ~477 lines)
   - `web/server.py` (WebSocket + broadcast, ~190 lines)
   - `src/indicators.py` `calc_spike_zscore` function
5. **Run simulator**: `python simulate.py your_data.csv --speed 10`
6. **Git workflow**:
   ```bash
   git add <files>
   git commit -m "description"
   git push -u origin claude/options-spike-detection-Y18qF
   ```
