from __future__ import annotations

import sqlite3
import pytest

from dispatch.db import StateStore
from dispatch.migrations import (
    CURRENT_SCHEMA_VERSION,
    MIGRATIONS,
    MigrationError,
    apply_migrations,
    get_schema_version,
)
from dispatch.models import Recipient


LEGACY_SCHEMA_V0 = """
CREATE TABLE IF NOT EXISTS recipients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    external_id TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    source_created_at TEXT,
    first_seen_at TEXT NOT NULL
);

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


def _get_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {r[0] for r in rows}


def _get_indexes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {r[0] for r in rows}


def test_fresh_database_initializes_at_latest_version(tmp_path):
    """TEST 1: fresh database reaches latest version and supports all operations."""
    db_path = str(tmp_path / "fresh.db")
    store = StateStore(db_path)

    # Verify PRAGMA user_version == CURRENT_SCHEMA_VERSION
    assert get_schema_version(db_path) == CURRENT_SCHEMA_VERSION

    with sqlite3.connect(db_path) as conn:
        tables = _get_tables(conn)
        indexes = _get_indexes(conn)

    assert "recipients" in tables
    assert "campaign_sends" in tables
    assert "campaign_recipients" in tables
    assert "idx_campaign_sends_claim" in indexes

    # Verify normal operations still work on the fresh database
    recipient = Recipient(email="alice@example.com", source="waitlist")
    recip_id, was_new = store.upsert_recipient(recipient, campaign="launch-2026")
    assert recip_id > 0
    assert was_new is True

    enqueued = store.enqueue(recip_id, "launch-2026")
    assert enqueued is True

    batch = store.claim_batch("launch-2026", limit=10)
    assert len(batch) == 1
    send_id = batch[0]["send_id"]

    store.mark_sent(send_id, provider_message_id="msg_fresh_001")
    assert store.campaign_stats("launch-2026") == {"sent": 1}


def test_legacy_database_upgrade_preserves_existing_data(tmp_path):
    """TEST 2: legacy unversioned database upgrades without data loss."""
    db_path = str(tmp_path / "legacy.db")

    # Construct a legacy database with user_version = 0 and realistic data
    with sqlite3.connect(db_path) as conn:
        conn.executescript(LEGACY_SCHEMA_V0)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0

        # Insert recipient
        conn.execute(
            """INSERT INTO recipients (id, email, source, external_id, metadata, source_created_at, first_seen_at)
               VALUES (1, 'legacy@example.com', 'csv', 'ext_99', '{"plan": "pro"}', '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')"""
        )
        # Insert campaign recipient
        conn.execute(
            """INSERT INTO campaign_recipients (campaign, recipient_id, imported_at)
               VALUES ('camp-old', 1, '2026-01-02T00:00:00Z')"""
        )
        # Insert queued/sending/failed/sent jobs
        conn.execute(
            """INSERT INTO campaign_sends
               (id, recipient_id, campaign, status, attempts, last_error, provider_message_id, next_attempt_at, sent_at, updated_at)
               VALUES (10, 1, 'camp-old', 'sent', 2, 'prior transient error', 'msg_legacy_123', NULL, '2026-01-03T12:00:00Z', '2026-01-03T12:00:00Z')"""
        )

    # Initialize StateStore on the legacy database
    store = StateStore(db_path)

    # Verify version was upgraded
    assert get_schema_version(db_path) == CURRENT_SCHEMA_VERSION

    # Verify all data was preserved exactly
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rec = conn.execute("SELECT * FROM recipients WHERE id = 1").fetchone()
        assert rec["email"] == "legacy@example.com"
        assert rec["source"] == "csv"
        assert rec["external_id"] == "ext_99"
        assert rec["metadata"] == '{"plan": "pro"}'
        assert rec["source_created_at"] == "2026-01-01T00:00:00Z"
        assert rec["first_seen_at"] == "2026-01-02T00:00:00Z"

        cr = conn.execute("SELECT * FROM campaign_recipients WHERE campaign = 'camp-old'").fetchone()
        assert cr["recipient_id"] == 1
        assert cr["imported_at"] == "2026-01-02T00:00:00Z"

        job = conn.execute("SELECT * FROM campaign_sends WHERE id = 10").fetchone()
        assert job["recipient_id"] == 1
        assert job["campaign"] == "camp-old"
        assert job["status"] == "sent"
        assert job["attempts"] == 2
        assert job["last_error"] == "prior transient error"
        assert job["provider_message_id"] == "msg_legacy_123"
        assert job["sent_at"] == "2026-01-03T12:00:00Z"
        assert job["updated_at"] == "2026-01-03T12:00:00Z"

    # Confirm stats and operations work on upgraded database
    assert store.campaign_stats("camp-old") == {"sent": 1}
    assert store.count_recipients("camp-old") == 1


def test_already_current_database_is_a_noop(tmp_path, monkeypatch):
    """TEST 3: already-current database runs no migrations and preserves schema/data."""
    db_path = str(tmp_path / "current.db")
    store = StateStore(db_path)
    assert get_schema_version(db_path) == CURRENT_SCHEMA_VERSION

    # Insert data
    recip_id, _ = store.upsert_recipient(Recipient(email="test@example.com"))
    store.enqueue(recip_id, "camp-noop")

    # Record schema snapshot
    with sqlite3.connect(db_path) as conn:
        schema_before = conn.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        rows_recipients_before = conn.execute("SELECT * FROM recipients").fetchall()
        rows_sends_before = conn.execute("SELECT * FROM campaign_sends").fetchall()

    # Spy on migration callable to verify no migration is invoked
    migration_invoked = []
    original_migration = MIGRATIONS[1]

    def spy_migration(conn):
        migration_invoked.append(1)
        return original_migration(conn)

    monkeypatch.setitem(MIGRATIONS, 1, spy_migration)

    # Initialize StateStore again
    store_second = StateStore(db_path)

    # Assert no migration was invoked
    assert len(migration_invoked) == 0

    # Assert schema and data remain identical
    with sqlite3.connect(db_path) as conn:
        schema_after = conn.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        rows_recipients_after = conn.execute("SELECT * FROM recipients").fetchall()
        rows_sends_after = conn.execute("SELECT * FROM campaign_sends").fetchall()

    assert schema_before == schema_after
    assert rows_recipients_before == rows_recipients_after
    assert rows_sends_before == rows_sends_after
    assert store_second.campaign_stats("camp-noop") == {"pending": 1}


def test_migration_failure_rolls_back(tmp_path):
    """TEST 4: failed migration rolls back transaction, leaving database usable and retryable."""
    db_path = str(tmp_path / "rollback.db")

    # Initialize database at baseline version 1
    store = StateStore(db_path)
    recip_id, _ = store.upsert_recipient(Recipient(email="stable@example.com"))
    store.enqueue(recip_id, "camp-stable")
    assert get_schema_version(db_path) == 1

    # Define a migration 2 that creates a table and then fails
    def migration_002_faulty(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE pending_change (id INT)")
        raise RuntimeError("Intentional migration 2 failure")

    test_migrations = {
        1: MIGRATIONS[1],
        2: migration_002_faulty,
    }

    # Attempt migration to version 2
    with pytest.raises(MigrationError, match="Migration 2 failed: Intentional migration 2 failure"):
        apply_migrations(db_path, migrations=test_migrations, target_version=2)

    # Verify version remains at 1
    assert get_schema_version(db_path) == 1

    # Verify pending_change table does NOT exist (was rolled back)
    with sqlite3.connect(db_path) as conn:
        tables = _get_tables(conn)
        assert "pending_change" not in tables
        # Existing data remains present
        rows = conn.execute("SELECT email FROM recipients").fetchall()
        assert rows == [("stable@example.com",)]

    # Reopening database still works
    store_reopened = StateStore(db_path)
    assert store_reopened.campaign_stats("camp-stable") == {"pending": 1}

    # Verify a subsequent successful migration can proceed
    def migration_002_fixed(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE pending_change (id INT)")

    test_migrations[2] = migration_002_fixed
    apply_migrations(db_path, migrations=test_migrations, target_version=2)
    assert get_schema_version(db_path) == 2
    with sqlite3.connect(db_path) as conn:
        assert "pending_change" in _get_tables(conn)


def test_unknown_future_version_raises_error(tmp_path):
    """TEST 5: unknown future version raises error and does not downgrade."""
    db_path = str(tmp_path / "future.db")

    # Create a database with version > CURRENT_SCHEMA_VERSION
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION + 1}")

    expected_msg = (
        f"Unsupported database schema version {CURRENT_SCHEMA_VERSION + 1}; "
        f"this application supports up to version {CURRENT_SCHEMA_VERSION}."
    )
    with pytest.raises(MigrationError, match=expected_msg):
        StateStore(db_path)

    # Verify database was not downgraded
    assert get_schema_version(db_path) == CURRENT_SCHEMA_VERSION + 1


def test_missing_migration_raises_error(tmp_path):
    """TEST 6: missing migration version in gap fails explicitly."""
    db_path = str(tmp_path / "missing.db")
    store = StateStore(db_path)
    assert get_schema_version(db_path) == 1

    # We want to migrate to target_version 3, but migration 2 is missing from dict
    def migration_003(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE v3_table (id INT)")

    incomplete_migrations = {
        1: MIGRATIONS[1],
        3: migration_003,
    }

    with pytest.raises(MigrationError, match="Missing migration for schema version 2."):
        apply_migrations(db_path, migrations=incomplete_migrations, target_version=3)

    # Ensure schema version was not advanced to 3
    assert get_schema_version(db_path) == 1
    with sqlite3.connect(db_path) as conn:
        assert "v3_table" not in _get_tables(conn)


def test_sequential_multi_step_migrations(tmp_path):
    """Verify that multiple pending migrations are applied in strict ascending order."""
    db_path = str(tmp_path / "multi.db")
    applied_order: list[int] = []

    def m1(conn: sqlite3.Connection) -> None:
        applied_order.append(1)
        conn.execute("CREATE TABLE step1 (id INT)")

    def m2(conn: sqlite3.Connection) -> None:
        applied_order.append(2)
        conn.execute("CREATE TABLE step2 (id INT)")

    def m3(conn: sqlite3.Connection) -> None:
        applied_order.append(3)
        conn.execute("CREATE TABLE step3 (id INT)")

    multi_migrations = {1: m1, 2: m2, 3: m3}

    apply_migrations(db_path, migrations=multi_migrations, target_version=3)

    assert applied_order == [1, 2, 3]
    assert get_schema_version(db_path) == 3
    with sqlite3.connect(db_path) as conn:
        tables = _get_tables(conn)
        assert {"step1", "step2", "step3"}.issubset(tables)


def test_get_schema_version_handles_both_connection_and_path(tmp_path):
    """get_schema_version works transparently with either connection or file path."""
    db_path = str(tmp_path / "test_version.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 42")
        assert get_schema_version(conn) == 42

    assert get_schema_version(db_path) == 42
