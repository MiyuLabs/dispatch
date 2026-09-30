from __future__ import annotations

import logging
import os
import sqlite3
from typing import Callable, Mapping, Optional

logger = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION: int = 1


class MigrationError(Exception):
    """Raised when a database migration fails or schema version is invalid."""


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


def migration_001_initial_schema(conn: sqlite3.Connection) -> None:
    """initial schema"""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recipients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL,
            external_id TEXT,
            metadata TEXT NOT NULL DEFAULT '{}',
            source_created_at TEXT,
            first_seen_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_campaign_sends_claim
            ON campaign_sends(campaign, status, next_attempt_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS campaign_recipients (
            campaign TEXT NOT NULL,
            recipient_id INTEGER NOT NULL REFERENCES recipients(id),
            imported_at TEXT NOT NULL,
            UNIQUE(campaign, recipient_id)
        )
        """
    )


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: migration_001_initial_schema,
}


def _migration_name(migration: Callable[[sqlite3.Connection], None]) -> str:
    doc = getattr(migration, "__doc__", None)
    if doc:
        return doc.strip().splitlines()[0]
    name = getattr(migration, "__name__", "")
    if name.startswith("migration_"):
        parts = name.split("_", 2)
        if len(parts) == 3:
            return parts[2].replace("_", " ")
    return name or "unnamed migration"


def get_schema_version(target: sqlite3.Connection | str | os.PathLike[str]) -> int:
    """Read the integer schema version from PRAGMA user_version."""
    if isinstance(target, sqlite3.Connection):
        row = target.execute("PRAGMA user_version").fetchone()
        return int(row[0]) if row else 0

    with sqlite3.connect(str(target)) as conn:
        row = conn.execute("PRAGMA user_version").fetchone()
        return int(row[0]) if row else 0


def apply_migrations(
    target: sqlite3.Connection | str | os.PathLike[str],
    migrations: Mapping[int, Callable[[sqlite3.Connection], None]] | None = None,
    target_version: int | None = None,
) -> None:
    """Apply pending migrations sequentially up to target_version inside transactions."""
    if migrations is None:
        migrations = MIGRATIONS
    if target_version is None:
        target_version = CURRENT_SCHEMA_VERSION

    if isinstance(target, (str, os.PathLike)):
        conn = sqlite3.connect(str(target), timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            _run_migrations(conn, migrations, target_version)
        finally:
            conn.close()
    else:
        _run_migrations(target, migrations, target_version)


def _run_migrations(
    conn: sqlite3.Connection,
    migrations: Mapping[int, Callable[[sqlite3.Connection], None]],
    target_version: int,
) -> None:
    original_isolation = conn.isolation_level
    try:
        conn.isolation_level = None
        while True:
            current_version = get_schema_version(conn)
            if current_version > target_version:
                raise MigrationError(
                    f"Unsupported database schema version {current_version}; "
                    f"this application supports up to version {target_version}."
                )
            if current_version == target_version:
                break

            next_version = current_version + 1
            if next_version not in migrations:
                raise MigrationError(f"Missing migration for schema version {next_version}.")

            migration = migrations[next_version]
            desc = _migration_name(migration)

            try:
                conn.execute("BEGIN IMMEDIATE")
                locked_version = get_schema_version(conn)
                if locked_version >= next_version:
                    conn.execute("COMMIT")
                    continue

                logger.info("Applying database migration %d: %s", next_version, desc)
                migration(conn)
                conn.execute(f"PRAGMA user_version = {next_version}")
                conn.execute("COMMIT")
            except Exception as exc:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise MigrationError(f"Migration {next_version} failed: {exc}") from exc
    finally:
        conn.isolation_level = original_isolation
