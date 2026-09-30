import os
import time
from typing import Optional
from unittest.mock import MagicMock

import pytest

from dispatch.config import Settings
from dispatch.db import StateStore
from dispatch.engine import SendEngine, enqueue_campaign
from dispatch.models import Recipient, RenderedEmail
from dispatch.providers.base import (
    AmbiguousProviderError,
    BatchItemResult,
    EmailJob,
    EmailProvider,
    PermanentProviderError,
    SendResult,
    TransientProviderError,
)
from dispatch.templates.base import Template


class _DummyTemplate(Template):
    name = "dummy"

    def render(self, recipient: Recipient) -> RenderedEmail:
        return RenderedEmail(subject=f"Subject for {recipient.email}", html="<p>hi</p>", text="hi")


def _settings(tmp_path, stale_seconds: float = 300.0) -> Settings:
    return Settings(
        resend_api_key="re_test",
        from_email="hi@example.com",
        from_name="Test",
        reply_to=None,
        db_path=str(tmp_path / "test.db"),
        rate_limit_per_second=1000.0,
        max_batch_size=10,
        max_attempts=3,
        base_backoff_seconds=1.0,
        max_backoff_seconds=10.0,
        sending_stale_after_seconds=stale_seconds,
        validate_deliverability=False,
        mx_lookup_timeout_seconds=1.0,
        pre_order_url="https://example.com",
        explore_url="https://example.com/explore",
    )


def _age_submissions(store: StateStore, seconds_ago: int = 301):
    from datetime import datetime, timedelta, timezone
    ts = (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()
    with store._connect() as conn:
        conn.execute("UPDATE provider_submissions SET updated_at = ?", (ts,))
        conn.execute("UPDATE campaign_sends SET updated_at = ?", (ts,))


class _RecordingProvider(EmailProvider):
    def __init__(self):
        self.calls: list[dict] = []
        self.outcomes: list = []  # list of outcomes to yield per chunk

    def send(self, **kwargs) -> SendResult:
        return SendResult(provider_message_id="single-dummy")

    def send_batch(
        self,
        jobs: list[EmailJob],
        *,
        from_email: str,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        idempotency_key_prefix: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> list[BatchItemResult]:
        call_info = {
            "jobs": list(jobs),
            "refs": [j.ref for j in jobs],
            "idempotency_key": idempotency_key,
            "idempotency_key_prefix": idempotency_key_prefix,
        }
        self.calls.append(call_info)

        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            if callable(outcome):
                return outcome(jobs, idempotency_key)
            if isinstance(outcome, list):
                return outcome

        return [BatchItemResult(ref=j.ref, ok=True, message_id=f"msg-{j.ref}") for j in jobs]


# ==============================================================================
# TEST 1 — provider timeout after acceptance
# ==============================================================================

def test_provider_timeout_after_acceptance_preserves_identity_and_retries(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="a@example.com"), campaign="launch")
    store.upsert_recipient(Recipient(email="b@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    accepted_remotely = []

    def timeout_outcome(jobs, key):
        # Provider accepts send remotely, but connection drops before response
        accepted_remotely.append((key, [j.ref for j in jobs]))
        return [
            BatchItemResult(
                ref=j.ref,
                ok=False,
                error="TimeoutError: Connection timed out",
                permanent=False,
                ambiguous=True,
            )
            for j in jobs
        ]

    def success_outcome(jobs, key):
        # On retry, provider recognizes idempotency key and confirms delivery
        return [BatchItemResult(ref=j.ref, ok=True, message_id=f"resend-{j.ref}") for j in jobs]

    provider = _RecordingProvider()
    provider.outcomes = [timeout_outcome, success_outcome]
    settings = _settings(tmp_path, stale_seconds=300.0)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    # 1. First run triggers timeout
    stats = engine.run_once("launch")
    assert stats.claimed == 2
    assert stats.sent == 0
    assert len(provider.calls) == 1
    assert len(accepted_remotely) == 1
    first_key = provider.calls[0]["idempotency_key"]
    assert first_key is not None

    # Job does NOT become ordinary pending; it is tracked as submitted / ambiguous
    with store._connect() as conn:
        sends = conn.execute("SELECT id, status FROM campaign_sends").fetchall()
        assert all(row["status"] == "submitted" for row in sends)
        subs = conn.execute("SELECT id, idempotency_key, status FROM provider_submissions").fetchall()
        assert len(subs) == 1
        assert subs[0]["status"] == "ambiguous"
        assert subs[0]["idempotency_key"] == first_key

    # 2. Before stale threshold, recovery does not kick in
    stats2 = engine.run_once("launch")
    assert stats2.claimed == 0
    assert len(provider.calls) == 1

    # 3. Simulate passage of time past stale threshold
    _age_submissions(store, seconds_ago=350)

    # 4. Next run recovers the stale submission
    stats3 = engine.run_once("launch")
    assert stats3.sent == 2
    assert len(provider.calls) == 2

    # Verify identical idempotency key was reused
    second_key = provider.calls[1]["idempotency_key"]
    assert first_key == second_key
    assert provider.calls[0]["refs"] == provider.calls[1]["refs"]

    # Verify status is now sent
    assert store.campaign_stats("launch") == {"sent": 2}


# ==============================================================================
# TEST 2 — recovery does not generate a new idempotency key
# ==============================================================================

def test_recovery_does_not_generate_new_idempotency_key(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="user1@example.com"), campaign="launch")
    store.upsert_recipient(Recipient(email="user2@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    # Force ambiguous error on first call
    provider.outcomes = [
        [BatchItemResult(ref=j.ref, ok=False, error="504 Gateway Timeout", ambiguous=True)
         for j in [EmailJob(ref=1, to="x", subject="s", html="h", text="t"),
                   EmailJob(ref=2, to="y", subject="s", html="h", text="t")]],
        # second call succeeds
    ]

    settings = _settings(tmp_path, stale_seconds=60.0)
    engine1 = SendEngine(store, provider, _DummyTemplate(), settings)
    engine1.run_once("launch")

    assert len(provider.calls) == 1
    key_attempt_1 = provider.calls[0]["idempotency_key"]
    refs_attempt_1 = provider.calls[0]["refs"]
    assert key_attempt_1 is not None

    # Age past stale threshold and restart engine with fresh instance
    _age_submissions(store, seconds_ago=70)
    engine2 = SendEngine(store, provider, _DummyTemplate(), settings)
    engine2.run_once("launch")

    assert len(provider.calls) == 2
    key_attempt_2 = provider.calls[1]["idempotency_key"]
    refs_attempt_2 = provider.calls[1]["refs"]

    assert key_attempt_1 == key_attempt_2
    assert refs_attempt_1 == refs_attempt_2


# ==============================================================================
# TEST 3 — recovery does not regroup jobs
# ==============================================================================

def test_recovery_does_not_regroup_jobs(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    # Initial batch: 3 recipients
    for i in range(1, 4):
        store.upsert_recipient(Recipient(email=f"orig{i}@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    provider.outcomes = [
        # First call fails ambiguously
        [BatchItemResult(ref=i, ok=False, error="Connection reset", ambiguous=True) for i in range(1, 4)],
        # Subsequent calls succeed
    ]

    settings = _settings(tmp_path, stale_seconds=60.0)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)
    engine.run_once("launch")

    assert len(provider.calls) == 1
    assert provider.calls[0]["refs"] == [1, 2, 3]

    # Now add unrelated pending jobs 4 and 5
    store.upsert_recipient(Recipient(email="new4@example.com"), campaign="launch")
    store.upsert_recipient(Recipient(email="new5@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    # Age the submission so it is eligible for recovery
    _age_submissions(store, seconds_ago=100)

    # Run engine again
    engine.run_once("launch")

    # Provider should have been called twice in total:
    # 1. Recovery of the stale submission with exactly [1, 2, 3]
    # 2. Processing of the new pending jobs [4, 5]
    assert len(provider.calls) == 3 or len(provider.calls) == 2
    # Verify the recovered batch was NOT merged into [1, 2, 3, 4, 5]
    recovery_call = provider.calls[1]
    assert recovery_call["refs"] == [1, 2, 3]
    assert recovery_call["idempotency_key"] == provider.calls[0]["idempotency_key"]

    if len(provider.calls) == 3:
        new_jobs_call = provider.calls[2]
        assert new_jobs_call["refs"] == [4, 5]


# ==============================================================================
# TEST 4 — confirmed success remains sent
# ==============================================================================

def test_confirmed_success_remains_sent(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="ok@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    # Confirmed success with provider_message_id
    provider.outcomes = [
        [BatchItemResult(ref=1, ok=True, message_id="resend_msg_abc123")]
    ]
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    stats = engine.run_once("launch")
    assert stats.sent == 1
    assert len(provider.calls) == 1

    # Check database
    with store._connect() as conn:
        row = conn.execute("SELECT status, provider_message_id FROM campaign_sends WHERE id = 1").fetchone()
        assert row["status"] == "sent"
        assert row["provider_message_id"] == "resend_msg_abc123"

    # Age past stale threshold and run engine again
    _age_submissions(store, seconds_ago=400)
    stats2 = engine.run_once("launch")
    assert stats2.claimed == 0
    assert stats2.sent == 0
    # Provider is not revisited
    assert len(provider.calls) == 1


# ==============================================================================
# TEST 5 — confirmed permanent failure
# ==============================================================================

def test_confirmed_permanent_failure_becomes_dead(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="invalid@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    # Confirmed permanent error (e.g. 403 Forbidden or 422 Unprocessable)
    provider.outcomes = [
        [BatchItemResult(ref=1, ok=False, error="403 Domain not verified", permanent=True, ambiguous=False)]
    ]
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    stats = engine.run_once("launch")
    assert stats.failed_exhausted == 1
    assert stats.sent == 0

    with store._connect() as conn:
        row = conn.execute("SELECT status, attempts, last_error FROM campaign_sends WHERE id = 1").fetchone()
        assert row["status"] == "dead"
        assert "403" in row["last_error"]

    # Age past stale threshold and verify no unnecessary retry occurs
    _age_submissions(store, seconds_ago=400)
    stats2 = engine.run_once("launch")
    assert stats2.claimed == 0
    assert len(provider.calls) == 1


# ==============================================================================
# TEST 6 — confirmed retryable failure
# ==============================================================================

def test_confirmed_retryable_failure_retains_backoff(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="retry@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    # Confirmed retryable failure (e.g. 429 Rate Limit)
    provider.outcomes = [
        [BatchItemResult(ref=1, ok=False, error="429 Rate limited", permanent=False, ambiguous=False, retry_after_seconds=5.0)],
        # second attempt succeeds
        [BatchItemResult(ref=1, ok=True, message_id="resend_msg_after_retry")],
    ]
    settings = _settings(tmp_path)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    stats = engine.run_once("launch")
    assert stats.failed_retryable == 1

    # Verify status is failed with next_attempt_at in the future
    with store._connect() as conn:
        row = conn.execute("SELECT status, attempts, next_attempt_at FROM campaign_sends WHERE id = 1").fetchone()
        assert row["status"] == "failed"
        assert row["attempts"] == 1
        assert row["next_attempt_at"] is not None

    # Before next_attempt_at: job is not claimed
    stats_early = engine.run_once("launch")
    assert stats_early.claimed == 0
    assert len(provider.calls) == 1

    # Fast-forward next_attempt_at to now
    with store._connect() as conn:
        conn.execute("UPDATE campaign_sends SET next_attempt_at = datetime('now', '-10 seconds') WHERE id = 1")

    # Job is claimed and succeeds
    stats_retry = engine.run_once("launch")
    assert stats_retry.claimed == 1
    assert stats_retry.sent == 1
    assert len(provider.calls) == 2


# ==============================================================================
# TEST 7 — stale threshold is configurable
# ==============================================================================

def test_stale_threshold_is_configurable(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="cfg@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    provider.outcomes = [
        [BatchItemResult(ref=1, ok=False, error="Timeout", ambiguous=True)],
        [BatchItemResult(ref=1, ok=True, message_id="recovered-msg")],
    ]

    # Set custom threshold of 45 seconds
    settings = _settings(tmp_path, stale_seconds=45.0)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    engine.run_once("launch")
    assert len(provider.calls) == 1

    # Age by 20 seconds (< 45s threshold) -> not stale, no recovery
    _age_submissions(store, seconds_ago=20)
    engine.run_once("launch")
    assert len(provider.calls) == 1

    # Age by 50 seconds (> 45s threshold) -> stale, recovered!
    _age_submissions(store, seconds_ago=50)
    engine.run_once("launch")
    assert len(provider.calls) == 2
    assert store.campaign_stats("launch") == {"sent": 1}


# ==============================================================================
# TEST 8 — ambiguous jobs are not silently recycled to pending
# ==============================================================================

def test_ambiguous_jobs_not_silently_recycled_to_pending(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="safe@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    provider.outcomes = [
        [BatchItemResult(ref=1, ok=False, error="Timeout", ambiguous=True)]
    ]
    settings = _settings(tmp_path, stale_seconds=60.0)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    engine.run_once("launch")

    # Age the submission
    _age_submissions(store, seconds_ago=120)

    # Calling legacy recover_stale_sending_jobs directly must NOT move submitted jobs to pending
    recovered_count = store.recover_stale_sending_jobs(campaign="launch", older_than_seconds=60)
    assert recovered_count == 0

    with store._connect() as conn:
        row = conn.execute("SELECT status FROM campaign_sends WHERE id = 1").fetchone()
        assert row["status"] == "submitted"


# ==============================================================================
# TEST 9 — repeated recovery is deterministic
# ==============================================================================

def test_repeated_recovery_is_deterministic(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    for i in (1, 2):
        store.upsert_recipient(Recipient(email=f"det{i}@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    # 3 consecutive ambiguous timeouts
    provider.outcomes = [
        [BatchItemResult(ref=i, ok=False, error="Timeout 1", ambiguous=True) for i in (1, 2)],
        [BatchItemResult(ref=i, ok=False, error="Timeout 2", ambiguous=True) for i in (1, 2)],
        [BatchItemResult(ref=i, ok=False, error="Timeout 3", ambiguous=True) for i in (1, 2)],
    ]
    settings = _settings(tmp_path, stale_seconds=30.0)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    # Run 1: initial send fails ambiguously
    engine.run_once("launch")
    assert len(provider.calls) == 1
    key1 = provider.calls[0]["idempotency_key"]
    refs1 = provider.calls[0]["refs"]

    # Run 2: recovery attempt 1 fails ambiguously
    _age_submissions(store, seconds_ago=40)
    engine.run_once("launch")
    assert len(provider.calls) == 2
    key2 = provider.calls[1]["idempotency_key"]
    refs2 = provider.calls[1]["refs"]

    # Run 3: recovery attempt 2 fails ambiguously
    _age_submissions(store, seconds_ago=40)
    engine.run_once("launch")
    assert len(provider.calls) == 3
    key3 = provider.calls[2]["idempotency_key"]
    refs3 = provider.calls[2]["refs"]

    # All attempts used identical key and identical membership
    assert key1 == key2 == key3
    assert refs1 == refs2 == refs3 == [1, 2]

    # No duplicate submission records created
    with store._connect() as conn:
        subs = conn.execute("SELECT count(*) as cnt FROM provider_submissions").fetchone()
        assert subs["cnt"] == 1


# ==============================================================================
# TEST 10 — provider message ID after recovery
# ==============================================================================

def test_provider_message_id_persisted_after_recovery(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    store.upsert_recipient(Recipient(email="persisted@example.com"), campaign="launch")
    enqueue_campaign(store, "launch")

    provider = _RecordingProvider()
    provider.outcomes = [
        # First attempt: ambiguous timeout
        [BatchItemResult(ref=1, ok=False, error="Connection timeout", ambiguous=True)],
        # Second attempt: recovery succeeds with provider message ID
        [BatchItemResult(ref=1, ok=True, message_id="resend_msg_recov_999")],
    ]
    settings = _settings(tmp_path, stale_seconds=50.0)
    engine = SendEngine(store, provider, _DummyTemplate(), settings)

    engine.run_once("launch")
    _age_submissions(store, seconds_ago=60)
    engine.run_once("launch")

    with store._connect() as conn:
        send_row = conn.execute("SELECT status, provider_message_id FROM campaign_sends WHERE id = 1").fetchone()
        assert send_row["status"] == "sent"
        assert send_row["provider_message_id"] == "resend_msg_recov_999"

        sub_row = conn.execute("SELECT status FROM provider_submissions").fetchone()
        assert sub_row["status"] == "completed"


# ==============================================================================
# TEST 11 — configuration tests
# ==============================================================================

def test_settings_sending_stale_after_seconds_parsing(monkeypatch):
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setenv("FROM_EMAIL", "test@example.com")

    # Default
    s1 = Settings.from_env()
    assert s1.sending_stale_after_seconds == 300.0

    # Custom valid float
    monkeypatch.setenv("SENDING_STALE_AFTER_SECONDS", "450.5")
    s2 = Settings.from_env()
    assert s2.sending_stale_after_seconds == 450.5

    # Invalid float raises ValueError
    monkeypatch.setenv("SENDING_STALE_AFTER_SECONDS", "not-a-number")
    with pytest.raises(ValueError):
        Settings.from_env()
