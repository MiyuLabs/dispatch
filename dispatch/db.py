from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

from .models import Recipient

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS recipients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    external_id TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    source_created_at TEXT,
    first_seen_at TEXT NOT NULL
);

-- One row per (recipient, campaign). This table IS the job queue: a
-- 'pending' row, or a 'failed' row whose next_attempt_at has elapsed, is a
-- unit of work waiting to be claimed. Because it lives in SQLite, a crashed
-- or interrupted run picks up exactly where it left off with no separate
-- broker required, and the UNIQUE constraint makes "don't email someone
-- twice for the same campaign" structurally impossible to violate.
CREATE TABLE IF NOT EXISTS campaign_sends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_id INTEGER NOT NULL REFERENCES recipients(id),
    campaign TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    provider_message_id TEXT,
    next_attempt_at TEXT,
    sent_at TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE(recipient_id, campaign)
);

CREATE INDEX IF NOT EXISTS idx_campaign_sends_claim
    ON campaign_sends(campaign, status, next_attempt_at);

CREATE TABLE IF NOT EXISTS campaign_recipients (
    campaign TEXT NOT NULL,
    recipient_id INTEGER NOT NULL REFERENCES recipients(id),
    imported_at TEXT NOT NULL,
    UNIQUE(campaign, recipient_id)
);
"""

# Valid statuses:
#   pending -> sending -> sent                         (success)
#                       -> failed  -> sending (retried) (transient, will retry — next_attempt_at is set)
#                       -> dead                         (transient retries exhausted, or a permanent
#                                                         error — next_attempt_at is NULL, never reclaimed)
#                       -> skipped                      (manual, e.g. unsubscribed)
TERMINAL_STATUSES = {"sent", "skipped", "dead"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StateStore:
    """Durable, file-based state for recipients and campaign sends.

    Safe to share across threads: each call opens its own short-lived
    connection (SQLite handles the file locking; WAL mode keeps readers
    and the single writer from blocking each other).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    # ---- recipients -----------------------------------------------------

    def upsert_recipient(self, r: Recipient, campaign: Optional[str] = None) -> tuple[int, bool]:
        """Insert a recipient if new. Returns (recipient_id, was_new)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM recipients WHERE email = ?", (r.email,)
            ).fetchone()
            if row:
                recipient_id = row["id"]
                was_new = False
            else:
                cur = conn.execute(
                    """INSERT INTO recipients
                           (email, source, external_id, metadata, source_created_at, first_seen_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        r.email,
                        r.source,
                        r.external_id,
                        json.dumps(r.metadata, default=str),
                        r.created_at.isoformat() if r.created_at else None,
                        _now(),
                    ),
                )
                recipient_id = cur.lastrowid
                was_new = True

            if campaign:
                try:
                    conn.execute(
                        "INSERT INTO campaign_recipients (campaign, recipient_id, imported_at) VALUES (?, ?, ?)",
                        (campaign, recipient_id, _now()),
                    )
                except sqlite3.IntegrityError:
                    pass

            return recipient_id, was_new

    def count_recipients(self, campaign: Optional[str] = None) -> int:
        with self._connect() as conn:
            if campaign:
                return conn.execute("SELECT COUNT(*) AS n FROM campaign_recipients WHERE campaign = ?", (campaign,)).fetchone()["n"]
            return conn.execute("SELECT COUNT(*) AS n FROM recipients").fetchone()["n"]

    def all_recipient_ids(self) -> list[int]:
        with self._connect() as conn:
            return [r["id"] for r in conn.execute("SELECT id FROM recipients").fetchall()]

    def all_recipients(self) -> list[sqlite3.Row]:
        """Full rows (not just ids) — used at enqueue time when a
        validator needs email/metadata to decide whether a recipient is
        worth sending to."""
        with self._connect() as conn:
            return conn.execute(
                "SELECT id, email, source, external_id, metadata, source_created_at FROM recipients"
            ).fetchall()

    def unenqueued_recipients(self, campaign: str) -> list[sqlite3.Row]:
        """Recipients who do not yet have a record in campaign_sends for this campaign."""
        with self._connect() as conn:
            return conn.execute(
                """SELECT r.id, r.email, r.source, r.external_id, r.metadata, r.source_created_at
                   FROM recipients r
                   JOIN campaign_recipients cr ON r.id = cr.recipient_id
                   WHERE cr.campaign = ? AND NOT EXISTS (
                       SELECT 1 FROM campaign_sends cs
                       WHERE cs.recipient_id = r.id AND cs.campaign = ?
                   )
                   ORDER BY r.id""",
                (campaign, campaign),
            ).fetchall()

    # ---- queue: enqueue ---------------------------------------------------

    def enqueue(self, recipient_id: int, campaign: str) -> bool:
        """Create a pending job for (recipient, campaign) if one doesn't
        already exist. Returns True iff a new job was created — call this
        freely on every run; it's idempotent."""
        with self._connect() as conn:
            try:
                conn.execute(
                    """INSERT INTO campaign_sends (recipient_id, campaign, status, updated_at)
                       VALUES (?, ?, 'pending', ?)""",
                    (recipient_id, campaign, _now()),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def enqueue_skipped(self, recipient_id: int, campaign: str, reason: str) -> bool:
        """Create a job that's already terminal — used for recipients that
        fail deliverability validation, so they're recorded (and visible in
        `dispatch status`) without ever consuming a send attempt or a
        provider API call. Same idempotency guarantee as `enqueue()`."""
        with self._connect() as conn:
            try:
                conn.execute(
                    """INSERT INTO campaign_sends (recipient_id, campaign, status, last_error, updated_at)
                       VALUES (?, ?, 'skipped', ?, ?)""",
                    (recipient_id, campaign, reason[:2000], _now()),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    # ---- queue: claim / complete / fail ------------------------------------

    def claim_batch(self, campaign: str, limit: int) -> list[sqlite3.Row]:
        """Atomically move up to `limit` eligible jobs to 'sending' and
        return them. Eligible = pending, or failed with an elapsed backoff
        and attempts remaining."""
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT cs.id AS send_id, cs.attempts,
                          r.id AS recipient_id, r.email, r.source,
                          r.external_id, r.metadata, r.source_created_at
                   FROM campaign_sends cs
                   JOIN recipients r ON r.id = cs.recipient_id
                   WHERE cs.campaign = ?
                     AND (
                            cs.status = 'pending'
                         OR (cs.status = 'failed' AND cs.next_attempt_at IS NOT NULL AND cs.next_attempt_at <= ?)
                     )
                   ORDER BY cs.id
                   LIMIT ?""",
                (campaign, now, limit),
            ).fetchall()
            if rows:
                ids = [r["send_id"] for r in rows]
                placeholders = ",".join("?" * len(ids))
                conn.execute(
                    f"UPDATE campaign_sends SET status='sending', updated_at=? WHERE id IN ({placeholders})",
                    (now, *ids),
                )
            conn.execute("COMMIT")
            return rows

    def pending_jobs_for_preview(self, campaign: str, limit: int = 500) -> list[sqlite3.Row]:
        """Read pending or due-for-retry jobs without modifying their status to 'sending' —
        used by --dry-run so local rendering and inspection is completely non-destructive."""
        now = _now()
        with self._connect() as conn:
            return conn.execute(
                """SELECT cs.id AS send_id, cs.attempts,
                          r.id AS recipient_id, r.email, r.source,
                          r.external_id, r.metadata, r.source_created_at
                   FROM campaign_sends cs
                   JOIN recipients r ON r.id = cs.recipient_id
                   WHERE cs.campaign = ?
                     AND (
                            cs.status = 'pending'
                         OR (cs.status = 'failed' AND cs.next_attempt_at IS NOT NULL AND cs.next_attempt_at <= ?)
                     )
                   ORDER BY cs.id
                   LIMIT ?""",
                (campaign, now, limit),
            ).fetchall()

    def recover_stale_sending_jobs(self, campaign: str, older_than_seconds: float = 300) -> int:
        """Reset jobs stuck in 'sending' (e.g. from an interrupted or crashed run)
        back to 'pending' if updated_at is older than `older_than_seconds` ago."""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat()
        now = _now()
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE campaign_sends
                   SET status='pending', updated_at=?
                   WHERE campaign=? AND status='sending' AND updated_at<=?""",
                (now, campaign, cutoff),
            )
            return cur.rowcount

    def mark_sent(self, send_id: int, provider_message_id: Optional[str]) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE campaign_sends
                   SET status='sent', provider_message_id=?, sent_at=?, updated_at=?
                   WHERE id=?""",
                (provider_message_id, _now(), _now(), send_id),
            )

    def mark_skipped(self, send_id: int, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE campaign_sends SET status='skipped', last_error=?, updated_at=? WHERE id=?""",
                (reason[:2000], _now(), send_id),
            )

    def mark_failed(
        self,
        send_id: int,
        attempts: int,
        error: str,
        next_attempt_at: Optional[str],
    ) -> None:
        """Record a failed attempt. If `next_attempt_at` is given, the job
        goes back to 'failed' and will be reclaimed once that time passes.
        If it's None — retries exhausted, or the error was classified as
        permanent — the job is marked 'dead' and will never be reclaimed."""
        status = "failed" if next_attempt_at is not None else "dead"
        with self._connect() as conn:
            conn.execute(
                """UPDATE campaign_sends
                   SET status=?, attempts=?, last_error=?, next_attempt_at=?, updated_at=?
                   WHERE id=?""",
                (status, attempts, error[:2000], next_attempt_at, _now(), send_id),
            )

    # ---- reporting --------------------------------------------------------

    def campaign_stats(self, campaign: str) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM campaign_sends WHERE campaign=? GROUP BY status",
                (campaign,),
            ).fetchall()
            return {r["status"]: r["n"] for r in rows}

    def failed_jobs(self, campaign: str, limit: int = 50) -> list[sqlite3.Row]:
        """Both 'failed' (still retrying) and 'dead' (exhausted/permanent)
        jobs — everything worth a human's attention."""
        with self._connect() as conn:
            return conn.execute(
                """SELECT cs.id, cs.status, r.email, cs.attempts, cs.last_error, cs.updated_at
                   FROM campaign_sends cs JOIN recipients r ON r.id = cs.recipient_id
                   WHERE cs.campaign=? AND cs.status IN ('failed', 'dead')
                   ORDER BY cs.updated_at DESC LIMIT ?""",
                (campaign, limit),
            ).fetchall()

    def skipped_jobs(self, campaign: str, limit: int = 50) -> list[sqlite3.Row]:
        """Recipients that never became a send attempt — filtered out by a
        RecipientValidator at enqueue time, or manually via mark_skipped()."""
        with self._connect() as conn:
            return conn.execute(
                """SELECT cs.id, r.email, cs.last_error, cs.updated_at
                   FROM campaign_sends cs JOIN recipients r ON r.id = cs.recipient_id
                   WHERE cs.campaign=? AND cs.status='skipped'
                   ORDER BY cs.updated_at DESC LIMIT ?""",
                (campaign, limit),
            ).fetchall()
