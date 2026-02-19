"""
test_backfill.py — Run this directly to diagnose backfill API issues.

Usage:
    python test_backfill.py

It will:
  1. Call intraday_minute_data for the Nifty 50 index (security_id=13, IDX_I)
     and for a known Nifty option security ID (if one was resolved at startup).
  2. Print the raw response so you can see exactly what the API returns.
"""
import json
import sys
from datetime import date, timedelta

from dotenv import load_dotenv
import os

load_dotenv()

DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")

if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    print("[ERROR] Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in your .env file")
    sys.exit(1)

from dhanhq import dhanhq
dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)


def prior_trading_days(n: int):
    today = date.today()
    found = 0
    ref = today - timedelta(days=1)
    while found < n - 1:
        if ref.weekday() < 5:
            found += 1
        if found < n - 1:
            ref -= timedelta(days=1)
    return ref.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


from_date, to_date = prior_trading_days(5)
print(f"\nDate range: {from_date} → {to_date}\n")

# ── Test 1: Nifty index (IDX_I / INDEX) ───────────────────────────────────────
print("=" * 60)
print("TEST 1: Nifty index  security_id=13  IDX_I  INDEX")
print("=" * 60)
resp = dhan.intraday_minute_data(
    security_id="13",
    exchange_segment="IDX_I",
    instrument_type="INDEX",
    from_date=from_date,
    to_date=to_date,
)
print(f"status  : {resp.get('status')}")
print(f"remarks : {resp.get('remarks')}")
data = resp.get("data", {})
print(f"data type: {type(data).__name__}")
if isinstance(data, dict):
    print(f"data keys: {list(data.keys())[:10]}")
    closes = data.get("close", data.get("data", {}).get("close", []) if isinstance(data.get("data"), dict) else [])
    print(f"close bars: {len(closes)}")
    if closes:
        print(f"first close: {closes[0]},  last close: {closes[-1]}")
else:
    print(f"raw data: {str(data)[:400]}")

# ── Test 2: NSE_FNO option (use a known security ID from the log) ─────────────
# Change this to a security_id seen in your startup log (e.g. 45569 for NIFTY ATM CE)
OPTION_SECURITY_ID = "45569"
print()
print("=" * 60)
print(f"TEST 2: Nifty option  security_id={OPTION_SECURITY_ID}  NSE_FNO  OPTIDX")
print("=" * 60)
resp2 = dhan.intraday_minute_data(
    security_id=OPTION_SECURITY_ID,
    exchange_segment="NSE_FNO",
    instrument_type="OPTIDX",
    from_date=from_date,
    to_date=to_date,
)
print(f"status  : {resp2.get('status')}")
print(f"remarks : {resp2.get('remarks')}")
data2 = resp2.get("data", {})
print(f"data type: {type(data2).__name__}")
if isinstance(data2, dict):
    print(f"data keys: {list(data2.keys())[:10]}")
    closes2 = data2.get("close", data2.get("data", {}).get("close", []) if isinstance(data2.get("data"), dict) else [])
    print(f"close bars: {len(closes2)}")
    if closes2:
        print(f"first close: {closes2[0]},  last close: {closes2[-1]}")
else:
    print(f"raw data: {str(data2)[:400]}")

# ── Test 3: Full raw dump of a successful response ────────────────────────────
print()
print("=" * 60)
print("TEST 3: Full raw data dict (first 800 chars)")
print("=" * 60)
target = resp if resp.get("status") == "success" else resp2
print(json.dumps(target.get("data", {}), default=str)[:800])
