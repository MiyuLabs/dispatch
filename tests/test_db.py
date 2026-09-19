from dispatch.db import StateStore
from dispatch.models import Recipient


def _store(tmp_path):
    return StateStore(str(tmp_path / "test.db"))


def test_upsert_recipient_dedupes_by_email(tmp_path):
    store = _store(tmp_path)
    r = Recipient(email="a@example.com", source="waitlist")
    id1, new1 = store.upsert_recipient(r)
    id2, new2 = store.upsert_recipient(r)
    assert id1 == id2
    assert new1 is True
    assert new2 is False
    assert store.count_recipients() == 1


def test_enqueue_is_idempotent_per_campaign(tmp_path):
    store = _store(tmp_path)
    rid, _ = store.upsert_recipient(Recipient(email="a@example.com"))
    assert store.enqueue(rid, "launch") is True
    assert store.enqueue(rid, "launch") is False  # already queued
    assert store.enqueue(rid, "another-campaign") is True  # different campaign is fine


def test_claim_batch_moves_pending_to_sending_and_hides_them(tmp_path):
    store = _store(tmp_path)
    rid, _ = store.upsert_recipient(Recipient(email="a@example.com"))
    store.enqueue(rid, "launch")

    batch = store.claim_batch("launch", limit=10)
    assert len(batch) == 1

    # claimed job is 'sending' now, not eligible for a second claim
    again = store.claim_batch("launch", limit=10)
    assert again == []


def test_mark_failed_with_no_next_attempt_is_not_reclaimed(tmp_path):
    store = _store(tmp_path)
    rid, _ = store.upsert_recipient(Recipient(email="a@example.com"))
    store.enqueue(rid, "launch")
    row = store.claim_batch("launch", limit=10)[0]

    store.mark_failed(row["send_id"], attempts=5, error="permanent", next_attempt_at=None)
    assert store.claim_batch("launch", limit=10) == []
    assert store.campaign_stats("launch") == {"dead": 1}


def test_mark_failed_with_future_next_attempt_is_reclaimed_later(tmp_path):
    from datetime import datetime, timedelta, timezone

    store = _store(tmp_path)
    rid, _ = store.upsert_recipient(Recipient(email="a@example.com"))
    store.enqueue(rid, "launch")
    row = store.claim_batch("launch", limit=10)[0]

    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    store.mark_failed(row["send_id"], attempts=1, error="transient", next_attempt_at=past)

    retried = store.claim_batch("launch", limit=10)
    assert len(retried) == 1


def test_mark_sent_is_terminal(tmp_path):
    store = _store(tmp_path)
    rid, _ = store.upsert_recipient(Recipient(email="a@example.com"))
    store.enqueue(rid, "launch")
    row = store.claim_batch("launch", limit=10)[0]

    store.mark_sent(row["send_id"], provider_message_id="msg_123")
    assert store.campaign_stats("launch") == {"sent": 1}
    assert store.claim_batch("launch", limit=10) == []
