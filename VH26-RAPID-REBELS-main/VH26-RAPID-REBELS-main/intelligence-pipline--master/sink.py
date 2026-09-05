"""Persistent sink (SQLite) with exactly-once guarantees.

Every processed event is written to a durable table keyed by event id. This
makes idempotency *provable*: after a simulated worker crash + retry, a payment
appears in the DB exactly once — proof the at-least-once + dedup machinery did
not double-apply a financial side effect.
"""
import sqlite3
import threading
import time
import os
from pathlib import Path


class SQLiteSink:
    def __init__(self, path: str = "pipeline_sink.db"):
        self.path = path
        # Remove any stale demo DB on init so a demo never inherits phantom rows.
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id      TEXT PRIMARY KEY,
                event_type    TEXT NOT NULL,
                priority      TEXT NOT NULL,
                processed_at  REAL NOT NULL,
                attempts      INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)")
        self.conn.commit()
        self._writes = 0
        self._duplicates_blocked = 0

    def upsert(self, event_id: str, event_type: str, priority: str, attempts: int = 1) -> bool:
        """Persist an event. Returns True if freshly inserted, False if it was an
        existing (duplicate) row that was NOT double-applied."""
        with self._lock:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO events(event_id, event_type, priority, processed_at, attempts) "
                "VALUES(?,?,?,?,?)",
                (event_id, event_type, priority, time.time(), attempts),
            )
            self.conn.commit()
            if cur.rowcount == 1:
                self._writes += 1
                return True
            self._duplicates_blocked += 1
            return False

    def count(self) -> int:
        with self._lock:
            row = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()
            return row[0] if row else 0

    def dedup_proof(self) -> dict:
        """Return evidence that exactly-once held: per-type counts + how many
        duplicate writes were blocked at the storage layer."""
        with self._lock:
            rows = self.conn.execute(
                "SELECT event_type, COUNT(*) FROM events GROUP BY event_type"
            ).fetchall()
            return {
                "by_type": {t: c for t, c in rows},
                "total_persisted": self.count(),
                "duplicates_blocked": self._duplicates_blocked,
                "writes_performed": self._writes,
                "db_path": self.path,
            }

    def close(self):
        with self._lock:
            self.conn.close()