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
                        df = pd.read_csv(INSTRUMENTS_CACHE_FILE, low_memory=False)
                        self._df = df
                        self._resolve_columns()
                        logger.info(f"Loaded instruments master from cache ({len(self._df)} rows)")
                        return True
                    except Exception as e:
                        logger.warning(f"Cache read failed: {e}. Re-downloading.")
                        self._df = None
                        self._col_map = {}

            return self._download()

    def _download(self) -> bool:
        try:
            logger.info(f"Downloading instruments master from {DHAN_INSTRUMENTS_CSV_URL} ...")
            resp = requests.get(DHAN_INSTRUMENTS_CSV_URL, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text), low_memory=False)
            self._df = df
            self._resolve_columns()
            # Save cache
            self._df.to_csv(INSTRUMENTS_CACHE_FILE, index=False)
            logger.info(f"Downloaded {len(self._df)} instruments, cache saved.")
            return True
        except Exception as e:
            logger.error(f"Failed to download instruments master: {e}", exc_info=True)
            self._df = None   # ← reset so is-None checks work correctly
            self._col_map = {}
            return False

    def _resolve_columns(self) -> None:
        """
        Map logical column names to actual CSV column names.
        1. Try exact match against known aliases (case-insensitive).
        2. Fall back to fuzzy substring matching for critical columns.
        Always logs the actual CSV columns so we can diagnose mismatches.
        """
        cols = list(self._df.columns)
        cols_upper_map = {c.upper(): c for c in cols}  # upper → actual name
        logger.info(f"Instruments CSV columns ({len(cols)}): {cols}")

        # Pass 1: exact match against aliases (case-insensitive)
        for logical, candidates in self._COL_ALIASES.items():
            for c in candidates:
                if c.upper() in cols_upper_map:
                    self._col_map[logical] = cols_upper_map[c.upper()]
                    break

        # Pass 2: fuzzy substring match for any still-missing columns
        _FUZZY: dict[str, callable] = {
            "security_id": lambda u: ("SECURITY" in u and "ID" in u) or u.endswith("_ID"),
            "symbol":      lambda u: "SYMBOL" in u,
            "instrument":  lambda u: "INSTRUMENT" in u,
            "expiry":      lambda u: "EXPIRY" in u or "EXPIR" in u,
            "strike":      lambda u: "STRIKE" in u,
            "option_type": lambda u: "OPTION" in u and "TYPE" in u,
            "segment":     lambda u: "SEGMENT" in u,
        }
        for logical, test_fn in _FUZZY.items():
            if logical not in self._col_map:
                hits = [c for c in cols if test_fn(c.upper())]
                if hits:
                    self._col_map[logical] = hits[0]
                    logger.info(f"Fuzzy-matched column '{logical}' → '{hits[0]}'")

        logger.info(f"Column map resolved: {self._col_map}")

        missing = [k for k in ["security_id", "symbol", "expiry", "strike", "option_type"]
                   if k not in self._col_map]
        if missing:
            raise ValueError(
                f"Could not find columns for: {missing}. "
                f"Available columns: {cols}"
            )

    def find_option(
        self,
        underlying: str,      # e.g. "NIFTY"
        expiry_date: str,      # "YYYY-MM-DD"
        strike: int,
        option_type: str,      # "CE" or "PE"
    ) -> Optional[OptionInfo]:
        """
        Find the Dhan security ID for a specific Nifty option.
        1. Filter by symbol prefix, instrument type (OPTIDX), option type, strike.
        2. Parse expiry column once (auto-detect format) and filter to nearest
           upcoming expiry — preferring exact match, falling back to the closest
           available future expiry in the CSV.
        Returns None if not found.
        """
        if self._df is None:
            return None

        cm = self._col_map
        df = self._df

        # --- symbol prefix + option type --------------------------------
        mask = (
            df[cm["symbol"]].str.upper().str.startswith(underlying.upper())
            & (df[cm["option_type"]].str.upper() == option_type.upper())
        )

        # Filter to index options (OPTIDX) when the instrument column is available.
        if "instrument" in cm:
            idx_mask = df[cm["instrument"]].str.upper().str.contains("OPTIDX", na=False)
            if idx_mask.any():
                mask &= idx_mask

        # --- strike ------------------------------------------------------
        try:
            strike_col = df[cm["strike"]].astype(float)
            mask &= (strike_col == float(strike))
        except Exception:
            pass

        candidates = df[mask].copy()
        if candidates.empty:
            logger.warning(
                f"No instrument found for {underlying} {strike}{option_type} "
                f"(expiry={expiry_date}) — no rows match symbol/instrument/strike"
            )
            return None

        # --- expiry: parse once, auto-detect format ----------------------
        target_dt = datetime.strptime(expiry_date, "%Y-%m-%d").date()
        today = datetime.now().date()

        raw_expiry = candidates[cm["expiry"]]
        parsed_expiry = pd.to_datetime(raw_expiry, dayfirst=True, errors="coerce")
        # If auto-detect failed for some rows, try explicit formats
        if parsed_expiry.isna().all():
            for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%b %d %Y"):
                try:
                    parsed_expiry = pd.to_datetime(raw_expiry, format=fmt, errors="coerce")
                    if not parsed_expiry.isna().all():
                        break
                except Exception:
                    continue

        candidates = candidates.copy()
        candidates["_expiry_dt"] = parsed_expiry.dt.date

        # Drop rows where expiry couldn't be parsed or is in the past
        candidates = candidates[
            candidates["_expiry_dt"].notna()
            & (candidates["_expiry_dt"] >= today)
        ]

        if candidates.empty:
            logger.warning(
                f"No current/future expiry found for {underlying} {strike}{option_type} "
                f"(target={expiry_date})"
            )
            return None

        # Prefer exact match; fall back to nearest upcoming expiry
        exact = candidates[candidates["_expiry_dt"] == target_dt]
        if not exact.empty:
            row = exact.iloc[0]
            matched_expiry = target_dt
        else:
            # Pick row with the closest future expiry date
            candidates = candidates.sort_values("_expiry_dt")
            row = candidates.iloc[0]
            matched_expiry = row["_expiry_dt"]
            logger.warning(
                f"Exact expiry {expiry_date} not found for {underlying} {strike}{option_type} "
                f"— using nearest available: {matched_expiry}"
            )

        sec_id = str(row[cm["security_id"]])
        try:
            sec_id = str(int(float(sec_id)))
        except (ValueError, TypeError):
            pass
        symbol = str(row[cm["symbol"]])

        logger.info(
            f"Resolved {underlying} {strike}{option_type}: "
            f"security_id={sec_id} symbol={symbol} expiry={matched_expiry}"
        )
        return OptionInfo(
            security_id=sec_id,
            symbol=symbol,
            strike=strike,
            option_type=option_type,
            expiry=str(matched_expiry),
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

    def initialise_from_dhan(self, dhan_client) -> bool:
        """Fallback: load instruments master via dhanhq.fetch_security_list()."""
        try:
            logger.info("Trying fetch_security_list('compact') for Nifty instruments master…")
            df = dhan_client.fetch_security_list('compact')
            if df is None or df.empty:
                logger.warning("fetch_security_list returned empty DataFrame")
                return False
            logger.info(f"fetch_security_list: {len(df)} rows, columns: {list(df.columns)}")
            self._master._df = df
            self._master._col_map = {}
            self._master._resolve_columns()
            logger.info(f"Nifty instruments master col_map: {self._master._col_map}")
            return True
        except Exception as e:
            logger.error(f"Nifty instruments master fallback failed: {e}", exc_info=True)
            return False

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
