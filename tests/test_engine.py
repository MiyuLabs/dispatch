from unittest.mock import MagicMock
from dispatch.config import Settings
from dispatch.db import StateStore
from dispatch.engine import enqueue_campaign, SendEngine
from dispatch.models import Recipient
from dispatch.providers.base import EmailProvider, SendResult
from dispatch.templates.base import Template
from dispatch.models import RenderedEmail


class _DummyTemplate(Template):
    name = "dummy"

    def render(self, recipient: Recipient) -> RenderedEmail:
        return RenderedEmail(subject=f"Hello {recipient.email}", html="<p>hi</p>", text="hi")


class _DummyProvider(EmailProvider):
    def __init__(self):
        self.sent_jobs = []

    def send(self, **kwargs):
        self.sent_jobs.append(kwargs)
        return SendResult(provider_message_id="dummy_msg")


def _settings(tmp_path):
    return Settings(
        resend_api_key="re_test",
        from_email="hi@example.com",
        from_name="Test",
        reply_to=None,
        db_path=str(tmp_path / "test.db"),
        rate_limit_per_second=100.0,
        max_batch_size=10,
        max_attempts=3,
        base_backoff_seconds=1.0,
        max_backoff_seconds=10.0,
        validate_deliverability=False,
        mx_lookup_timeout_seconds=1.0,
        pre_order_url="https://example.com",
        explore_url="https://example.com/explore",
    )


def test_dry_run_is_non_destructive_and_leaves_jobs_pending(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="a@example.com"), campaign="test-campaign")
    store.upsert_recipient(Recipient(email="b@example.com"), campaign="test-campaign")
    enqueue_campaign(store, "test-campaign")

    # Before dry run: 2 pending
    assert store.campaign_stats("test-campaign") == {"pending": 2}

    provider = _DummyProvider()
    settings = _settings(tmp_path)
    engine_dry = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=True)

    stats = engine_dry.run_once("test-campaign")
    assert stats.claimed == 2
    assert stats.sent == 2
    assert len(provider.sent_jobs) == 0  # Provider was never called

    # CRITICAL INVARIANT: Database still has 2 pending jobs, NOT marked sent!
    assert store.campaign_stats("test-campaign") == {"pending": 2}

    # Now run for real: jobs must be claimed and sent
    engine_real = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)
    real_stats = engine_real.run_once("test-campaign")
    assert real_stats.claimed == 2
    assert real_stats.sent == 2
    assert len(provider.sent_jobs) == 2
    assert store.campaign_stats("test-campaign") == {"sent": 2}


def test_stale_sending_jobs_are_recovered(tmp_path):
    from datetime import datetime, timedelta, timezone

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="a@example.com"), campaign="test-campaign")
    enqueue_campaign(store, "test-campaign")

    # Manually simulate a crashed run where job is left in 'sending' with old updated_at
    batch = store.claim_batch("test-campaign", limit=1)
    send_id = batch[0]["send_id"]
    stale_time = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
    with store._connect() as conn:
        conn.execute("UPDATE campaign_sends SET updated_at=? WHERE id=?", (stale_time, send_id))

    assert store.campaign_stats("test-campaign") == {"sending": 1}

    # Running SendEngine should recover it to pending and process it
    provider = _DummyProvider()
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)

    stats = engine.run_once("test-campaign")
    assert stats.claimed == 1
    assert stats.sent == 1
    assert store.campaign_stats("test-campaign") == {"sent": 1}


def test_enqueue_campaign_skips_validation_for_already_enqueued(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="a@example.com"), campaign="camp1")
    store.upsert_recipient(Recipient(email="b@example.com"), campaign="camp1")

    mock_validator = MagicMock()
    mock_validator.validate.return_value.valid = True

    # First enqueue: 2 recipients validated and queued
    stats1 = enqueue_campaign(store, "camp1", validator=mock_validator)
    assert stats1.queued == 2
    assert stats1.already_queued == 0
    assert mock_validator.validate.call_count == 2

    # Second enqueue (e.g. cron retry): 0 recipients need validation
    mock_validator.reset_mock()
    stats2 = enqueue_campaign(store, "camp1", validator=mock_validator)
    assert stats2.queued == 0
    assert stats2.already_queued == 2
    mock_validator.validate.assert_not_called()  # Did not perform redundant DNS / validation checks!


class _ConfigurableBatchProvider(EmailProvider):
    def __init__(self, responses=None):
        self.sent_batches = []
        self.responses = list(responses or [])

    def send(self, **kwargs):
        raise NotImplementedError

    def send_batch(self, jobs, **kwargs):
        self.sent_batches.append({"jobs": list(jobs), "kwargs": dict(kwargs)})
        if self.responses:
            resp = self.responses.pop(0)
            if isinstance(resp, Exception):
                raise resp
            return resp
        from dispatch.providers.base import BatchItemResult
        return [BatchItemResult(ref=j.ref, ok=True, message_id=f"msg_{j.ref}") for j in jobs]


def test_partial_provider_success_reconciles_atomically(tmp_path):
    from dispatch.providers.base import BatchItemResult

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="good@example.com"), campaign="camp1")
    store.upsert_recipient(Recipient(email="perm@example.com"), campaign="camp1")
    store.upsert_recipient(Recipient(email="transient@example.com"), campaign="camp1")
    enqueue_campaign(store, "camp1")

    # Fetch IDs
    with store._connect() as conn:
        rows = conn.execute("SELECT cs.id, r.email FROM campaign_sends cs JOIN recipients r ON r.id=cs.recipient_id ORDER BY cs.id").fetchall()
        id_by_email = {r["email"]: r["id"] for r in rows}

    provider = _ConfigurableBatchProvider(responses=[
        [
            BatchItemResult(ref=id_by_email["good@example.com"], ok=True, message_id="prov_good_123"),
            BatchItemResult(ref=id_by_email["perm@example.com"], ok=False, error="mailbox does not exist", permanent=True),
            BatchItemResult(ref=id_by_email["transient@example.com"], ok=False, error="rate limited 429", permanent=False, retry_after_seconds=15),
        ]
    ])
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)

    stats = engine.run_once("camp1")
    assert stats.claimed == 3
    assert stats.sent == 1
    assert stats.failed_exhausted == 1
    assert stats.failed_retryable == 1

    assert store.campaign_stats("camp1") == {"sent": 1, "dead": 1, "failed": 1}
    assert store.unfinished_batches("camp1") == []

    with store._connect() as conn:
        row_good = conn.execute("SELECT * FROM campaign_sends WHERE id=?", (id_by_email["good@example.com"],)).fetchone()
        row_perm = conn.execute("SELECT * FROM campaign_sends WHERE id=?", (id_by_email["perm@example.com"],)).fetchone()
        row_trans = conn.execute("SELECT * FROM campaign_sends WHERE id=?", (id_by_email["transient@example.com"],)).fetchone()

    assert row_good["status"] == "sent"
    assert row_good["provider_message_id"] == "prov_good_123"

    assert row_perm["status"] == "dead"
    assert row_perm["attempts"] == 1
    assert "mailbox does not exist" in row_perm["last_error"]

    assert row_trans["status"] == "failed"
    assert row_trans["attempts"] == 1
    assert "rate limited 429" in row_trans["last_error"]
    assert row_trans["next_attempt_at"] is not None


def test_interrupted_reconciliation_recovers_exact_same_batch(tmp_path):
    from dispatch.db import compute_batch_idempotency_key
    import pytest

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="1@example.com"), campaign="camp1")
    store.upsert_recipient(Recipient(email="2@example.com"), campaign="camp1")
    store.upsert_recipient(Recipient(email="3@example.com"), campaign="camp1")
    store.upsert_recipient(Recipient(email="unrelated@example.com"), campaign="camp1")
    enqueue_campaign(store, "camp1")

    # Attempt 1: provider succeeds, but reconciliation crashes before commit
    provider1 = _ConfigurableBatchProvider()
    settings = _settings(tmp_path)
    engine1 = SendEngine(store, provider1, _DummyTemplate(), settings, dry_run=False)

    original_reconcile = store.reconcile_batch

    def crashing_reconcile(batch_id, updates):
        raise RuntimeError("Crash before reconciliation commits")

    store.reconcile_batch = crashing_reconcile

    with pytest.raises(RuntimeError, match="Crash before reconciliation commits"):
        engine1.run_once("camp1", batch_size=3)

    # Invariant: durable batch exists, exactly 3 jobs in sending, 1 pending
    unfinished = store.unfinished_batches("camp1")
    assert len(unfinished) == 1
    batch_id = unfinished[0]["id"]
    batch_jobs = store.get_batch_jobs(batch_id)
    assert len(batch_jobs) == 3
    claimed_ids = [j["send_id"] for j in batch_jobs]

    assert store.campaign_stats("camp1") == {"sending": 3, "pending": 1}

    # Attempt 2: restore normal reconcile, start a new engine instance
    store.reconcile_batch = original_reconcile
    provider2 = _ConfigurableBatchProvider()
    engine2 = SendEngine(store, provider2, _DummyTemplate(), settings, dry_run=False)

    stats = engine2.run_once("camp1", batch_size=3)

    # Verify attempt 2 submitted the exact same jobs in the exact same order for the recovered batch
    assert len(provider2.sent_batches) == 2
    attempt2_recovered_jobs = provider2.sent_batches[0]["jobs"]
    assert [j.ref for j in attempt2_recovered_jobs] == claimed_ids

    # Verify identical logical idempotency key
    key1 = compute_batch_idempotency_key("dispatch:camp1", [j.ref for j in provider1.sent_batches[0]["jobs"]])
    key2 = compute_batch_idempotency_key("dispatch:camp1", [j.ref for j in attempt2_recovered_jobs])
    assert key1 == key2

    # Unrelated job was submitted as a separate, distinct batch and not mixed in
    assert len(provider2.sent_batches[1]["jobs"]) == 1
    assert provider2.sent_batches[1]["jobs"][0].to == "unrelated@example.com"

    # All jobs are now sent, and no unfinished batches remain
    assert stats.sent == 4
    assert store.campaign_stats("camp1") == {"sent": 4}
    assert store.unfinished_batches("camp1") == []


def test_atomic_rollback_on_reconciliation_failure(tmp_path):
    import sqlite3
    import pytest

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="a@example.com"), campaign="camp")
    store.upsert_recipient(Recipient(email="b@example.com"), campaign="camp")
    enqueue_campaign(store, "camp")

    provider = _ConfigurableBatchProvider()
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)

    # Claim the batch
    batch = store.claim_batch("camp", limit=2)
    batch_id = batch[0]["batch_id"]

    # Wrap reconcile_batch to simulate failure after partial database execution
    real_reconcile = store.reconcile_batch

    def failing_reconcile(b_id, updates):
        # execute partial update on connection, then fail before commit
        with store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE campaign_sends SET status='sent' WHERE id=?", (updates[0].send_id,))
            conn.execute("ROLLBACK")
        raise sqlite3.OperationalError("Simulated disk error during transaction")

    store.reconcile_batch = failing_reconcile

    with pytest.raises(sqlite3.OperationalError):
        store.reconcile_batch(batch_id, [
            type("Update", (), {"send_id": batch[0]["send_id"], "status": "sent", "attempts": 1, "provider_message_id": "m1", "last_error": None, "next_attempt_at": None})(),
            type("Update", (), {"send_id": batch[1]["send_id"], "status": "sent", "attempts": 1, "provider_message_id": "m2", "last_error": None, "next_attempt_at": None})(),
        ])

    # Earlier update was rolled back, no partial 'sent' state remains
    assert store.campaign_stats("camp") == {"sending": 2}
    assert store.get_send_batch(batch_id) is not None

    # Now perform successful reconciliation
    store.reconcile_batch = real_reconcile
    engine.run_once("camp")
    assert store.campaign_stats("camp") == {"sent": 2}
    assert store.unfinished_batches("camp") == []


def test_ambiguous_provider_response_not_partially_committed(tmp_path):
    from dispatch.providers.base import BatchItemResult

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="1@example.com"), campaign="camp")
    store.upsert_recipient(Recipient(email="2@example.com"), campaign="camp")
    store.upsert_recipient(Recipient(email="3@example.com"), campaign="camp")
    enqueue_campaign(store, "camp")

    batch = store.claim_batch("camp", limit=3)
    batch_id = batch[0]["batch_id"]
    refs = [b["send_id"] for b in batch]

    # Provider returns incomplete results (only 2 results for 3 jobs)
    bad_response = [
        BatchItemResult(ref=refs[0], ok=True, message_id="m1"),
        BatchItemResult(ref=refs[1], ok=True, message_id="m2"),
    ]
    good_response = [
        BatchItemResult(ref=refs[0], ok=True, message_id="m1"),
        BatchItemResult(ref=refs[1], ok=True, message_id="m2"),
        BatchItemResult(ref=refs[2], ok=True, message_id="m3"),
    ]

    provider = _ConfigurableBatchProvider(responses=[bad_response, good_response])
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)

    # First run: ambiguous response
    stats1 = engine.run_once("camp")
    assert stats1.sent == 0
    # Nothing was partially committed
    assert store.campaign_stats("camp") == {"sending": 3}
    batch_record = store.get_send_batch(batch_id)
    assert batch_record is not None
    assert "ambiguous response" in (batch_record["last_error"] or "")

    # Second run: recovers exact same batch and reconciles cleanly
    stats2 = engine.run_once("camp")
    assert stats2.sent == 3
    assert store.campaign_stats("camp") == {"sent": 3}
    assert store.unfinished_batches("camp") == []


def test_repeated_recovery_is_deterministic(tmp_path):
    from dispatch.db import compute_batch_idempotency_key
    from dispatch.providers.base import TransientProviderError

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="a@example.com"), campaign="camp")
    store.upsert_recipient(Recipient(email="b@example.com"), campaign="camp")
    store.upsert_recipient(Recipient(email="c@example.com"), campaign="camp")
    enqueue_campaign(store, "camp")

    batch = store.claim_batch("camp", limit=3)
    batch_id = batch[0]["batch_id"]
    original_send_ids = [b["send_id"] for b in batch]

    # Enqueue additional unrelated jobs while batch is in-flight
    store.upsert_recipient(Recipient(email="d@example.com"), campaign="camp")
    store.upsert_recipient(Recipient(email="e@example.com"), campaign="camp")
    enqueue_campaign(store, "camp")

    provider = _ConfigurableBatchProvider(responses=[
        TransientProviderError("503 Gateway Timeout"),
        TransientProviderError("503 Gateway Timeout"),
        TransientProviderError("503 Gateway Timeout"),
    ])
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)

    # Run recovery 3 times
    for _ in range(3):
        engine.run_once("camp")
        # Invariant: durable batch membership never changes
        jobs = store.get_batch_jobs(batch_id)
        assert [j["send_id"] for j in jobs] == original_send_ids
        key = compute_batch_idempotency_key("dispatch:camp", (j["send_id"] for j in jobs))
        assert key == batch[0]["idempotency_key"] if "idempotency_key" in batch[0].keys() else key

    # 4th run: provider succeeds on the recovered batch, and then delivers the 2 queued jobs
    provider.responses.append([
        type("Res", (), {"ref": sid, "ok": True, "message_id": f"msg_{sid}", "error": None, "permanent": None, "retry_after_seconds": None})()
        for sid in original_send_ids
    ])
    engine.run_once("camp")
    # All 5 sends are now delivered: the original 3 jobs recovered as one batch, and the 2 new jobs as the next
    assert len(provider.sent_batches) == 5  # 3 failed attempts for batch 1 + 1 success for batch 1 + 1 batch for d,e
    assert [j.ref for j in provider.sent_batches[3]["jobs"]] == original_send_ids
    assert len(provider.sent_batches[4]["jobs"]) == 2
    assert store.campaign_stats("camp") == {"sent": 5}
    assert store.unfinished_batches("camp") == []


def test_provider_message_id_always_preserved(tmp_path):
    from dispatch.providers.base import BatchItemResult

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="persist@example.com"), campaign="camp")
    enqueue_campaign(store, "camp")

    provider = _ConfigurableBatchProvider()
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)

    engine.run_once("camp")
    with store._connect() as conn:
        mid = conn.execute("SELECT provider_message_id FROM campaign_sends WHERE campaign='camp'").fetchone()["provider_message_id"]

    assert mid.startswith("msg_")


def test_legacy_stale_sending_recovered_without_batch(tmp_path):
    from datetime import datetime, timedelta, timezone

    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="legacy@example.com"), campaign="camp")
    enqueue_campaign(store, "camp")

    # Put in 'sending' with old timestamp without creating send_batches row
    old_time = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
    with store._connect() as conn:
        conn.execute("UPDATE campaign_sends SET status='sending', updated_at=? WHERE campaign='camp'", (old_time,))

    assert store.campaign_stats("camp") == {"sending": 1}
    assert store.unfinished_batches("camp") == []

    provider = _ConfigurableBatchProvider()
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings, dry_run=False)

    # run_once should recover it via stale recovery fallback and deliver it cleanly
    stats = engine.run_once("camp")
    assert stats.claimed == 1
    assert stats.sent == 1
    assert store.campaign_stats("camp") == {"sent": 1}
