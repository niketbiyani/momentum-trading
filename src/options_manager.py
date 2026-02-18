"""
Options Manager — resolves Nifty ATM / ITM option security IDs via Dhan's
instruments master CSV and manages which four contracts to track.

Tracked instruments (for Nifty):
  1. ATM CE   — call at the nearest strike to Nifty spot
  2. ITM CE   — one strike BELOW ATM for calls (deeper in the money)
  3. ATM PE   — put at the nearest strike to Nifty spot
  4. ITM PE   — one strike ABOVE ATM for puts  (deeper in the money)

The manager watches for ATM strike changes and re-resolves security IDs
whenever the underlying moves through a strike boundary.
"""
import io
import os
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

import requests
import pandas as pd

from config import (
    NIFTY_STRIKE_STEP,
    DHAN_INSTRUMENTS_CSV_URL,
    INSTRUMENTS_CACHE_FILE,
)
from src.models import OptionInfo

logger = logging.getLogger(__name__)


# ── Expiry helpers ─────────────────────────────────────────────────────────────

def nearest_nifty_expiry(reference: Optional[datetime] = None) -> str:
    """
    Return the nearest upcoming Nifty weekly expiry (Thursday) as YYYY-MM-DD.
    If today is Thursday and it's past 15:30 IST, return NEXT Thursday.
    """
    now = reference or datetime.now()
    # Thursday = weekday 3
    days_ahead = (3 - now.weekday()) % 7
    if days_ahead == 0:
        # It's Thursday — check if market has closed (after 15:30)
        if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
            days_ahead = 7   # Already expired today, use next week
    candidate = now + timedelta(days=days_ahead)
    return candidate.strftime("%Y-%m-%d")


def atm_strike(spot_price: float, step: int = NIFTY_STRIKE_STEP) -> int:
    """Round spot price to nearest strike step."""
    return int(round(spot_price / step) * step)


# ── Instruments master ─────────────────────────────────────────────────────────

class InstrumentsMaster:
    """
    Downloads and caches Dhan's instruments master CSV.
    Provides lookup for Nifty option security IDs.

    Column names in the Dhan CSV (as of 2024-25):
      SEM_SMST_SECURITY_ID, SEM_TRADING_SYMBOL, SEM_INSTRUMENT_NAME,
      SEM_EXPIRY_DATE, SEM_STRIKE_PRICE, SEM_OPTION_TYPE,
      SEM_EXM_EXCH_ID, SEM_SEGMENT, SEM_LOT_SIZE, ...
    """

    # Alternative column name mappings (Dhan occasionally renames columns)
    _COL_ALIASES = {
        "security_id": ["SEM_SMST_SECURITY_ID", "SECURITY_ID", "SecurityId"],
        "symbol":      ["SEM_TRADING_SYMBOL",   "TRADING_SYMBOL", "Symbol"],
        "instrument":  ["SEM_INSTRUMENT_NAME",  "INSTRUMENT_NAME", "InstrumentName"],
        "expiry":      ["SEM_EXPIRY_DATE",       "EXPIRY_DATE", "ExpiryDate"],
        "strike":      ["SEM_STRIKE_PRICE",      "STRIKE_PRICE", "StrikePrice"],
        "option_type": ["SEM_OPTION_TYPE",       "OPTION_TYPE", "OptionType"],
        "exchange":    ["SEM_EXM_EXCH_ID",       "EXCHANGE", "Exchange"],
        "segment":     ["SEM_SEGMENT",           "SEGMENT", "Segment"],
    }

    def __init__(self):
        self._df: Optional[pd.DataFrame] = None
        self._col_map: dict[str, str] = {}
        self._lock = threading.Lock()

    def load(self, force_refresh: bool = False) -> bool:
        """
        Load the instruments master. Uses a local cache if available and recent.
        Returns True on success.
        """
        with self._lock:
            if not force_refresh and os.path.exists(INSTRUMENTS_CACHE_FILE):
                mtime = os.path.getmtime(INSTRUMENTS_CACHE_FILE)
                age_hours = (datetime.now().timestamp() - mtime) / 3600
                if age_hours < 12:  # Cache is fresh (< 12 hours old)
                    try:
                        self._df = pd.read_csv(INSTRUMENTS_CACHE_FILE, low_memory=False)
                        self._resolve_columns()
                        logger.info(f"Loaded instruments master from cache ({len(self._df)} rows)")
                        return True
                    except Exception as e:
                        logger.warning(f"Cache read failed: {e}. Re-downloading.")

            return self._download()

    def _download(self) -> bool:
        try:
            logger.info(f"Downloading instruments master from {DHAN_INSTRUMENTS_CSV_URL} ...")
            resp = requests.get(DHAN_INSTRUMENTS_CSV_URL, timeout=30)
            resp.raise_for_status()
            self._df = pd.read_csv(io.StringIO(resp.text), low_memory=False)
            self._resolve_columns()
            # Save cache
            self._df.to_csv(INSTRUMENTS_CACHE_FILE, index=False)
            logger.info(f"Downloaded {len(self._df)} instruments, cache saved.")
            return True
        except Exception as e:
            logger.error(f"Failed to download instruments master: {e}")
            return False

    def _resolve_columns(self) -> None:
        """Map our logical column names to actual CSV column names."""
        cols = set(self._df.columns)
        for logical, candidates in self._COL_ALIASES.items():
            for c in candidates:
                if c in cols:
                    self._col_map[logical] = c
                    break
        missing = [k for k in ["security_id", "symbol", "expiry", "strike", "option_type"]
                   if k not in self._col_map]
        if missing:
            raise ValueError(f"Could not find columns for: {missing}. "
                             f"Available: {list(self._df.columns)}")

    def find_option(
        self,
        underlying: str,      # e.g. "NIFTY"
        expiry_date: str,      # "YYYY-MM-DD"
        strike: int,
        option_type: str,      # "CE" or "PE"
    ) -> Optional[OptionInfo]:
        """
        Find the Dhan security ID for a specific Nifty option.
        Returns None if not found.
        """
        if self._df is None:
            return None

        cm = self._col_map
        df = self._df

        # Filter on symbol containing underlying name, option type, segment NFO
        mask = (
            df[cm["symbol"]].str.contains(underlying, na=False, case=False)
            & (df[cm["option_type"]].str.upper() == option_type.upper())
        )

        # Strike (may be stored as float or int)
        try:
            strike_col = df[cm["strike"]].astype(float)
            mask &= (strike_col == float(strike))
        except Exception:
            pass

        # Expiry — Dhan may store as "DD-Mon-YYYY" or "YYYY-MM-DD"
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

        matches = df[mask]
        if matches.empty:
            logger.warning(
                f"No instrument found for {underlying} {strike}{option_type} expiry={expiry_date}"
            )
            return None

        row = matches.iloc[0]
        sec_id = str(row[cm["security_id"]])
        symbol = str(row[cm["symbol"]])

        return OptionInfo(
            security_id=sec_id,
            symbol=symbol,
            strike=strike,
            option_type=option_type,
            expiry=expiry_date,
            label="",  # Set by caller
        )


# ── Options Manager ────────────────────────────────────────────────────────────

class OptionsManager:
    """
    High-level manager that:
      1. Determines current ATM strike from Nifty spot price.
      2. Resolves and returns the 4 OptionInfo objects to track.
      3. Detects ATM changes when Nifty crosses a strike boundary.
    """

    def __init__(self):
        self._master = InstrumentsMaster()
        self._nifty_spot: float = 0.0
        self._current_atm: int = 0
        self._tracked: list[OptionInfo] = []
        self._expiry: str = ""
        self._lock = threading.Lock()

    def initialise(self) -> bool:
        """Load instruments master. Must be called once before resolve()."""
        return self._master.load()

    def update_spot(self, spot_price: float) -> bool:
        """
        Update Nifty spot price. Returns True if ATM strike changed
        (caller should re-subscribe to new options).
        """
        with self._lock:
            self._nifty_spot = spot_price
            new_atm = atm_strike(spot_price)
            if new_atm != self._current_atm:
                self._current_atm = new_atm
                return True
            return False

    def resolve_instruments(self, spot_price: Optional[float] = None) -> list[OptionInfo]:
        """
        Return the 4 OptionInfo objects for the current ATM strike.

        Instruments:
          0: ATM CE   — strike = ATM,          CE
          1: ITM CE   — strike = ATM - step,   CE  (one strike ITM for calls)
          2: ATM PE   — strike = ATM,          PE
          3: ITM PE   — strike = ATM + step,   PE  (one strike ITM for puts)
        """
        if spot_price is not None:
            self.update_spot(spot_price)

        with self._lock:
            if self._nifty_spot <= 0:
                logger.error("Nifty spot price not set. Call update_spot() first.")
                return []

            expiry = nearest_nifty_expiry()
            self._expiry = expiry
            atm = self._current_atm or atm_strike(self._nifty_spot)
            self._current_atm = atm

        contracts = [
            (atm,                      "CE", "ATM CE"),
            (atm - NIFTY_STRIKE_STEP,  "CE", "ITM CE"),
            (atm,                      "PE", "ATM PE"),
            (atm + NIFTY_STRIKE_STEP,  "PE", "ITM PE"),
        ]

        instruments: list[OptionInfo] = []
        for strike, otype, label in contracts:
            info = self._master.find_option("NIFTY", expiry, strike, otype)
            if info:
                info.label = label
                instruments.append(info)
            else:
                # Fallback: create a placeholder so the rest of the system can run
                placeholder = OptionInfo(
                    security_id=f"NIFTY_{strike}{otype}_{expiry}",
                    symbol=f"NIFTY{expiry.replace('-','')}{strike}{otype}",
                    strike=strike,
                    option_type=otype,
                    expiry=expiry,
                    label=label + " (unresolved)",
                )
                instruments.append(placeholder)
                logger.warning(f"Using placeholder for {label} — security ID unresolved")

        with self._lock:
            self._tracked = instruments

        return instruments

    @property
    def tracked(self) -> list[OptionInfo]:
        with self._lock:
            return list(self._tracked)

    @property
    def nifty_spot(self) -> float:
        with self._lock:
            return self._nifty_spot

    @property
    def current_atm(self) -> int:
        with self._lock:
            return self._current_atm

    @property
    def expiry(self) -> str:
        with self._lock:
            return self._expiry
