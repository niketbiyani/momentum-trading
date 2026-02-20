import os
from dotenv import load_dotenv

load_dotenv()

# ── Dhan Credentials ──────────────────────────────────────────────────────────
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")

# ── Web Server ─────────────────────────────────────────────────────────────────
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))

# ── Spike Detection ───────────────────────────────────────────────────────────
SPIKE_THRESHOLD_PCT = float(os.getenv("SPIKE_THRESHOLD_PCT", "5.0"))
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "30"))
RSI_UP_MIN = float(os.getenv("RSI_UP_MIN", "45"))       # RSI must stay above this after up-spike
RSI_DOWN_MAX = float(os.getenv("RSI_DOWN_MAX", "55"))   # RSI must stay below this after down-spike

# ── Indicator Periods ─────────────────────────────────────────────────────────
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_EMA_PERIOD = int(os.getenv("RSI_EMA_PERIOD", "50"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL_PERIOD = int(os.getenv("MACD_SIGNAL", "9"))

# Minimum bars before each indicator produces a real value (not a seed/default).
# Below these counts the web state sends None so the UI shows "—" not 50/0.
RSI_MIN_BARS     = RSI_PERIOD + 1                       # 15 bars
RSI_EMA_MIN_BARS = RSI_PERIOD + RSI_EMA_PERIOD          # 64 bars
MACD_MIN_BARS    = MACD_SLOW  + MACD_SIGNAL_PERIOD      # 35 bars

# ── Lookback Table ─────────────────────────────────────────────────────────────
LOOKBACK_START = int(os.getenv("LOOKBACK_START", "10"))
LOOKBACK_END = int(os.getenv("LOOKBACK_END", "150"))
LOOKBACK_STEP = int(os.getenv("LOOKBACK_STEP", "10"))
LOOKBACK_PERIODS = list(range(LOOKBACK_START, LOOKBACK_END + 1, LOOKBACK_STEP))  # [10,20,..,150]

# ── Timeframes ─────────────────────────────────────────────────────────────────
TIMEFRAMES = {
    "5s":  5,
    "15s": 15,
    "1m":  60,
    "3m":  180,
}
# Which TF labels to show in each tab
NIFTY_TIMEFRAMES  = ["5s", "15s", "1m"]   # Nifty index options tab
STOCK_TIMEFRAMES  = ["1m", "3m"]           # Nifty 50 stocks tab

# For each TF, how many bars equals a 10-minute wall-clock window.
# Used so "Spk%" always shows the 10-min % move regardless of bar frequency,
# making the number directly comparable across 5s, 15s, and 1m columns.
# 3m has no better option — nearest valid lookback is pct[10] = 30 min.
TF_10MIN_BARS: dict[str, int] = {
    "5s":  120,   # 120 × 5s  = 600s = 10 min
    "15s":  40,   # 40  × 15s = 600s = 10 min
    "1m":   10,   # 10  × 1m  = 600s = 10 min
    "3m":   10,   # 10  × 3m  = 30 min (best available)
}

# ── Nifty / Options Settings ──────────────────────────────────────────────────
NIFTY_SECURITY_ID = "13"         # Nifty 50 index security ID on Dhan
NIFTY_STRIKE_STEP = int(os.getenv("NIFTY_STRIKE_STEP", "50"))
MAX_BARS = 400                   # Rolling bar history per timeframe (> LOOKBACK_END=150).
                                # 400 × 1m = ~6.5 h; enough for 2 days of context after

# ── Instruments Master ────────────────────────────────────────────────────────
DHAN_INSTRUMENTS_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
INSTRUMENTS_CACHE_FILE = ".instruments_cache.csv"

# ── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_REFRESH_INTERVAL = 0.5  # seconds

# ── Nifty 50 Constituent Stocks ───────────────────────────────────────────────
# Keys are NSE trading symbols. Values are F&O strike step sizes (in ₹).
# Stock options use monthly expiry (last Thursday of the month).
NIFTY50_STOCKS: dict[str, int] = {
    "ADANIENT":   50,
    "ADANIPORTS": 20,
    "APOLLOHOSP": 50,
    "ASIANPAINT": 50,
    "AXISBANK":   10,
    "BAJAJ-AUTO": 100,
    "BAJFINANCE": 100,
    "BAJAJFINSV": 50,
    "BPCL":       5,
    "BHARTIARTL": 20,
    "BRITANNIA":  100,
    "CIPLA":      20,
    "COALINDIA":  5,
    "DIVISLAB":   100,
    "DRREDDY":    100,
    "EICHERMOT":  100,
    "GRASIM":     50,
    "HCLTECH":    20,
    "HDFCBANK":   10,
    "HDFCLIFE":   10,
    "HEROMOTOCO": 100,
    "HINDALCO":   10,
    "HINDUNILVR": 50,
    "ICICIBANK":  10,
    "INDUSINDBK": 20,
    "INFY":       20,
    "ITC":        5,
    "JSWSTEEL":   20,
    "KOTAKBANK":  20,
    "LT":         50,
    "LTIM":       100,
    "MARUTI":     100,
    "NESTLEIND":  100,
    "NTPC":       5,
    "ONGC":       5,
    "POWERGRID":  5,
    "RELIANCE":   20,
    "SBILIFE":    20,
    "SHRIRAMFIN": 50,
    "SBIN":       10,
    "SUNPHARMA":  20,
    "TCS":        50,
    "TATACONSUM": 20,
    "TATAMOTORS": 20,
    "TATASTEEL":  5,
    "TECHM":      20,
    "TITAN":      50,
    "TRENT":      50,
    "ULTRACEMCO": 100,
    "WIPRO":      10,
}

# Backfill stock options at startup (set False to skip API calls for all 50 stocks)
BACKFILL_STOCK_OPTIONS = os.getenv("BACKFILL_STOCK_OPTIONS", "false").lower() == "true"
