"""
BarStore: persists completed 5s/15s bars to a local SQLite database.

Why: the Dhan REST API only provides 1-minute historical bars.  5s and 15s
bars only exist in memory and are lost on restart.  This module writes every
completed 5s/15s bar to disk as it closes, so the next restart can load them
back and seed the indicator engines immediately.

Design decisions:
  - SQLite (stdlib, zero-dependency)
  - Only 5s and 15s timeframes are persisted (1m comes from the API)
  - Bars older than KEEP_DAYS trading days are pruned on startup
  - Write uses INSERT OR REPLACE so restarts never produce duplicates
"""
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path

from src.models import Bar

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH        = Path(".bar_cache.db")
PERSIST_TFS    = {"5s", "15s"}   # only sub-minute TFs need persisting
KEEP_DAYS      = 3               # prune bars older than this many calendar days


class BarStore:
    """Thread-safe SQLite-backed bar store."""

    def __init__(self, db_path: Path = DB_PATH):
        self._db_path = db_path
        self._lock    = threading.Lock()
        self._init_db()

    # ── Setup ────────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bars (
                    security_id  TEXT    NOT NULL,
                    tf           TEXT    NOT NULL,
                    ts           INTEGER NOT NULL,
                    open         REAL    NOT NULL,
                    high         REAL    NOT NULL,
                    low          REAL    NOT NULL,
                    close        REAL    NOT NULL,
                    volume       INTEGER NOT NULL,
                    PRIMARY KEY (security_id, tf, ts)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_bars "
                "ON bars (security_id, tf, ts)"
            )

    # ── Write ────────────────────────────────────────────────────────────────

    def write_bar(self, security_id: str, tf: str, bar: Bar) -> None:
        """
        Persist a completed bar.  Silently skips timeframes not in PERSIST_TFS.
        Safe to call from any thread.
        """
        if tf not in PERSIST_TFS:
            return
        ts = int(bar.timestamp.timestamp())
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?)",
                    (security_id, tf, ts,
                     bar.open, bar.high, bar.low, bar.close, bar.volume),
                )

    # ── Read ─────────────────────────────────────────────────────────────────

    def load_bars(
        self,
        security_id: str,
        tf: str,
        max_bars: int = 400,
    ) -> list[Bar]:
        """
        Return the most recent `max_bars` bars for (security_id, tf) in
        chronological order (oldest → newest).  Returns [] if none found.
        """
        if tf not in PERSIST_TFS:
            return []
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT ts, open, high, low, close, volume
                    FROM   bars
                    WHERE  security_id = ? AND tf = ?
                    ORDER  BY ts DESC
                    LIMIT  ?
                    """,
                    (security_id, tf, max_bars),
                ).fetchall()

        # Rows came out newest-first; reverse for chronological order
        return [
            Bar(
                timestamp=datetime.fromtimestamp(ts),
                open=o, high=h, low=l, close=c, volume=int(v),
            )
            for ts, o, h, l, c, v in reversed(rows)
        ]

    # ── Maintenance ──────────────────────────────────────────────────────────

    def prune_old(self, keep_days: int = KEEP_DAYS) -> int:
        """
        Delete bars older than `keep_days` calendar days.
        Returns the number of rows deleted.
        """
        cutoff = int((datetime.now() - timedelta(days=keep_days)).timestamp())
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                cursor = conn.execute(
                    "DELETE FROM bars WHERE ts < ?", (cutoff,)
                )
                return cursor.rowcount
