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

# ── Lookback Table ─────────────────────────────────────────────────────────────
LOOKBACK_START = int(os.getenv("LOOKBACK_START", "10"))
LOOKBACK_END = int(os.getenv("LOOKBACK_END", "150"))
LOOKBACK_STEP = int(os.getenv("LOOKBACK_STEP", "10"))
LOOKBACK_PERIODS = list(range(LOOKBACK_START, LOOKBACK_END + 1, LOOKBACK_STEP))  # [10,20,..,150]

# ── Timeframes ─────────────────────────────────────────────────────────────────
TIMEFRAMES = {
    "5s": 5,
    "15s": 15,
    "1m": 60,
}

# ── Nifty / Options Settings ──────────────────────────────────────────────────
NIFTY_SECURITY_ID = "13"         # Nifty 50 index security ID on Dhan
NIFTY_STRIKE_STEP = int(os.getenv("NIFTY_STRIKE_STEP", "50"))
MAX_BARS = 200                   # Rolling bar history to keep (> LOOKBACK_END)

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
