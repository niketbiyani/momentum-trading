"""
Stock Options Manager — resolves ATM CE and ATM PE options for all 50
Nifty 50 constituent stocks via Dhan's instruments master CSV.

Key differences from Nifty index options:
  - Monthly expiry (last Thursday of the month, not weekly)
  - Each stock has its own strike step size
  - Spot prices come from NSE_EQ segment (not IDX_I)
  - Instrument type is OPTSTK (not OPTIDX)
"""
import logging
import threading
from calendar import monthrange
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from config import NIFTY50_STOCKS, DHAN_INSTRUMENTS_CSV_URL, INSTRUMENTS_CACHE_FILE
from src.models import OptionInfo
from src.options_manager import InstrumentsMaster   # reuse existing master loader

logger = logging.getLogger(__name__)


# ── Expiry helpers ─────────────────────────────────────────────────────────────

def last_thursday_of_month(year: int, month: int) -> datetime:
    """Return the last Thursday of a given month."""
    last_day = monthrange(year, month)[1]
    dt = datetime(year, month, last_day)
    # weekday(): Monday=0 … Thursday=3 … Sunday=6
    days_back = (dt.weekday() - 3) % 7
    return dt - timedelta(days=days_back)


def nearest_monthly_expiry(reference: Optional[datetime] = None) -> str:
    """
    Return the nearest upcoming monthly stock-option expiry (last Thursday
    of the month) as YYYY-MM-DD.

    If today's expiry has already passed (or market has closed on it), roll
    to the following month.
    """
    now = reference or datetime.now()
    expiry = last_thursday_of_month(now.year, now.month)

    expired = now.date() > expiry.date() or (
        now.date() == expiry.date()
        and now.hour >= 15
        and now.minute >= 30
    )

    if expired:
        # Roll to next month
        if now.month == 12:
            expiry = last_thursday_of_month(now.year + 1, 1)
        else:
            expiry = last_thursday_of_month(now.year, now.month + 1)

    return expiry.strftime("%Y-%m-%d")


# ── Stock spot security ID lookup ──────────────────────────────────────────────

class StockMasterMixin:
    """
    Extends InstrumentsMaster with equity (spot) security ID lookup and
    multi-stock option resolution.
    """

    def find_equity_security_id(self, symbol: str) -> Optional[str]:
        """
        Find the NSE_EQ (cash market) security ID for a stock symbol.
        Returns None if not found.
        """
        if self._df is None:
            return None

        cm = self._col_map
        df = self._df

        # Try exact symbol match in NSE_EQ / NSE segment
        sym_col = cm.get("symbol", "")
        seg_col = cm.get("segment", "")

        if not sym_col:
            return None

        mask = df[sym_col].str.upper() == symbol.upper()

        # Filter to equity segment if column is available
        if seg_col and seg_col in df.columns:
            eq_mask = df[seg_col].str.contains("NSE_EQ", na=False, case=False)
            if eq_mask.any():
                mask &= eq_mask

        matches = df[mask]
        if matches.empty:
            logger.debug(f"No EQ security found for {symbol}")
            return None

        return str(matches.iloc[0][cm["security_id"]])

    def find_stock_option(
        self,
        underlying: str,
        expiry_date: str,
        strike: int,
        option_type: str,
    ) -> Optional[OptionInfo]:
        """
        Find a stock option (OPTSTK) in the instruments master.
        Similar to find_option() but targets OPTSTK instruments.
        """
        if self._df is None:
            return None

        cm = self._col_map
        df = self._df

        mask = (
            df[cm["symbol"]].str.upper().str.startswith(underlying.upper(), na=False)
            & (df[cm["option_type"]].str.upper() == option_type.upper())
        )

        # Strike filter
        try:
            strike_col = df[cm["strike"]].astype(float)
            mask &= (strike_col == float(strike))
        except Exception:
            pass

        # Expiry filter
        expiry_dt = datetime.strptime(expiry_date, "%Y-%m-%d")
        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                parsed = pd.to_datetime(df[cm["expiry"]], format=fmt, errors="coerce")
                expiry_mask = parsed.dt.date == expiry_dt.date()
                if expiry_mask.any():
                    mask &= expiry_mask
                    break
            except Exception:
                continue

        # Prefer FNO segment rows
        seg_col = cm.get("segment", "")
        if seg_col and seg_col in df.columns:
            fno_mask = mask & df[seg_col].str.contains("FNO", na=False, case=False)
            if fno_mask.any():
                mask = fno_mask

        matches = df[mask]
        if matches.empty:
            logger.debug(
                f"No stock option found for {underlying} {strike}{option_type} "
                f"expiry={expiry_date}"
            )
            return None

        row = matches.iloc[0]
        return OptionInfo(
            security_id=str(row[cm["security_id"]]),
            symbol=str(row[cm["symbol"]]),
            strike=strike,
            option_type=option_type,
            expiry=expiry_date,
            label="",
            underlying=underlying,
        )


# ── Combined master (patched with stock helpers) ───────────────────────────────

class FullInstrumentsMaster(StockMasterMixin, InstrumentsMaster):
    """Instruments master with both Nifty-option and stock-option lookup."""
    pass


# ── Stock Options Manager ──────────────────────────────────────────────────────

class StockOptionsManager:
    """
    Resolves ATM CE + ATM PE for every Nifty 50 constituent stock.

    Usage:
        mgr = StockOptionsManager()
        mgr.initialise()                        # load instruments master
        spot_prices = {...}                     # {symbol: float}
        instruments, eq_ids = mgr.resolve_all(spot_prices)
    """

    def __init__(self):
        self._master = FullInstrumentsMaster()
        self._lock = threading.Lock()
        self._tracked: list[OptionInfo] = []
        self._eq_security_ids: dict[str, str] = {}   # symbol -> NSE_EQ sec ID

    def initialise(self) -> bool:
        return self._master.load()

    def resolve_all(
        self, spot_prices: dict[str, float]
    ) -> tuple[list[OptionInfo], dict[str, str]]:
        """
        Resolve ATM CE and ATM PE for each Nifty 50 stock.

        Args:
            spot_prices: {symbol: current_spot_price}

        Returns:
            (instruments, eq_security_ids)
            - instruments: list of OptionInfo objects (2 per stock = up to 100)
            - eq_security_ids: {symbol: NSE_EQ security_id} for feed subscription
        """
        expiry = nearest_monthly_expiry()
        instruments: list[OptionInfo] = []
        eq_ids: dict[str, str] = {}

        for symbol, strike_step in NIFTY50_STOCKS.items():
            spot = spot_prices.get(symbol, 0.0)
            if spot <= 0:
                logger.warning(f"No spot price for {symbol} — skipping")
                continue

            # Round to nearest strike
            atm = int(round(spot / strike_step) * strike_step)

            # Look up NSE_EQ security ID for spot feed
            eq_id = self._master.find_equity_security_id(symbol)
            if eq_id:
                eq_ids[symbol] = eq_id
            else:
                logger.warning(f"Could not find NSE_EQ security ID for {symbol}")

            # Resolve ATM CE and ATM PE
            for opt_type, label in [("CE", "ATM CE"), ("PE", "ATM PE")]:
                info = self._master.find_stock_option(symbol, expiry, atm, opt_type)
                if info:
                    info.label = f"{symbol} {label}"
                    info.underlying = symbol
                    instruments.append(info)
                else:
                    # Placeholder so the rest of the system can still run
                    placeholder = OptionInfo(
                        security_id=f"{symbol}_{atm}{opt_type}_{expiry}",
                        symbol=f"{symbol}{expiry.replace('-','')}{atm}{opt_type}",
                        strike=atm,
                        option_type=opt_type,
                        expiry=expiry,
                        label=f"{symbol} {label} (unresolved)",
                        underlying=symbol,
                    )
                    instruments.append(placeholder)
                    logger.warning(f"Placeholder for {symbol} {label}")

        with self._lock:
            self._tracked = instruments
            self._eq_security_ids = eq_ids

        logger.info(
            f"Resolved {len(instruments)} stock options across "
            f"{len(spot_prices)} stocks, expiry={expiry}"
        )
        return instruments, eq_ids

    def fetch_spot_prices(self, dhan_client) -> dict[str, float]:
        """
        Fetch current spot prices for all Nifty 50 stocks via Dhan REST API.
        Builds a mapping of {symbol: ltp}.
        Falls back gracefully if individual stocks fail.
        """
        spot_prices: dict[str, float] = {}
        symbols = list(NIFTY50_STOCKS.keys())

        # First, get equity security IDs from the instruments master
        eq_ids: dict[str, str] = {}
        for sym in symbols:
            sid = self._master.find_equity_security_id(sym)
            if sid:
                eq_ids[sym] = sid
            else:
                logger.debug(f"No EQ security ID found for {sym}")

        if not eq_ids:
            logger.warning("No equity security IDs found — cannot fetch spot prices")
            return spot_prices

        # Batch request via get_market_feed_quote
        try:
            sec_ids = [int(sid) for sid in eq_ids.values() if sid.isdigit()]
            resp = dhan_client.get_market_feed_quote(
                securities={"NSE_EQ": sec_ids}
            )
            data = resp.get("data", {}).get("NSE_EQ", {})
            # Build reverse map: security_id -> symbol
            rev = {sid: sym for sym, sid in eq_ids.items()}
            for sec_id_str, quote in data.items():
                ltp = float(quote.get("last_price", 0) or quote.get("LTP", 0))
                sym = rev.get(sec_id_str) or rev.get(str(int(float(sec_id_str))))
                if sym and ltp > 0:
                    spot_prices[sym] = ltp
            logger.info(f"Fetched spot prices for {len(spot_prices)}/{len(symbols)} stocks via batch API")
        except Exception as e:
            logger.warning(f"Batch spot fetch failed: {e}. Trying LTP endpoint...")

        # Fallback: individual LTP calls for any missing stocks
        missing = [sym for sym in symbols if sym not in spot_prices and sym in eq_ids]
        for sym in missing[:10]:   # limit fallback calls to avoid rate limits
            try:
                sid = eq_ids[sym]
                resp = dhan_client.get_last_traded_price(
                    securities={"NSE_EQ": [int(sid)]}
                )
                data = resp.get("data", {})
                for _, val in data.items():
                    ltp = float(val.get("LTP", 0) or val.get("last_price", 0))
                    if ltp > 0:
                        spot_prices[sym] = ltp
                        break
            except Exception as e:
                logger.debug(f"LTP fetch failed for {sym}: {e}")

        return spot_prices

    @property
    def eq_security_ids(self) -> dict[str, str]:
        with self._lock:
            return dict(self._eq_security_ids)
