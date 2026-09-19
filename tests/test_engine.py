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
