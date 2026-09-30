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


def test_claim_batch_creates_durable_send_batch_and_items(tmp_path):
    store = _store(tmp_path)
    r1, _ = store.upsert_recipient(Recipient(email="1@example.com"))
    r2, _ = store.upsert_recipient(Recipient(email="2@example.com"))
    store.enqueue(r1, "launch")
    store.enqueue(r2, "launch")

    batch = store.claim_batch("launch", limit=10)
    assert len(batch) == 2
    batch_id = batch[0]["batch_id"]
    assert batch[1]["batch_id"] == batch_id

    unfinished = store.unfinished_batches("launch")
    assert len(unfinished) == 1
    assert unfinished[0]["id"] == batch_id
    assert unfinished[0]["campaign"] == "launch"
    assert "dispatch:launch:batch:" in unfinished[0]["idempotency_key"]

    jobs = store.get_batch_jobs(batch_id)
    assert len(jobs) == 2
    assert [j["send_id"] for j in jobs] == [batch[0]["send_id"], batch[1]["send_id"]]


def test_reconcile_batch_atomic_commit_and_cleanup(tmp_path):
    from dispatch.db import JobReconciliationUpdate

    store = _store(tmp_path)
    r1, _ = store.upsert_recipient(Recipient(email="1@example.com"))
    r2, _ = store.upsert_recipient(Recipient(email="2@example.com"))
    store.enqueue(r1, "launch")
    store.enqueue(r2, "launch")

    batch = store.claim_batch("launch", limit=10)
    batch_id = batch[0]["batch_id"]

    updates = [
        JobReconciliationUpdate(send_id=batch[0]["send_id"], status="sent", attempts=1, provider_message_id="msg_1"),
        JobReconciliationUpdate(send_id=batch[1]["send_id"], status="dead", attempts=1, last_error="hard bounce"),
    ]
    store.reconcile_batch(batch_id, updates)

    assert store.campaign_stats("launch") == {"sent": 1, "dead": 1}
    assert store.unfinished_batches("launch") == []
    assert store.get_send_batch(batch_id) is None


def test_reconcile_batch_atomic_rollback(tmp_path):
    from dispatch.db import JobReconciliationUpdate
    import pytest

    store = _store(tmp_path)
    r1, _ = store.upsert_recipient(Recipient(email="1@example.com"))
    r2, _ = store.upsert_recipient(Recipient(email="2@example.com"))
    store.enqueue(r1, "launch")
    store.enqueue(r2, "launch")

    batch = store.claim_batch("launch", limit=10)
    batch_id = batch[0]["batch_id"]

    # One valid update, followed by one invalid status update that triggers ValueError
    bad_updates = [
        JobReconciliationUpdate(send_id=batch[0]["send_id"], status="sent", attempts=1, provider_message_id="msg_1"),
        JobReconciliationUpdate(send_id=batch[1]["send_id"], status="invalid_status", attempts=1),
    ]

    with pytest.raises(ValueError):
        store.reconcile_batch(batch_id, bad_updates)

    # Invariant: first update was rolled back, batch still exists
    assert store.campaign_stats("launch") == {"sending": 2}
    assert len(store.unfinished_batches("launch")) == 1
    assert store.get_send_batch(batch_id) is not None

    # Now reconcile cleanly
    good_updates = [
        JobReconciliationUpdate(send_id=batch[0]["send_id"], status="sent", attempts=1, provider_message_id="msg_1"),
        JobReconciliationUpdate(send_id=batch[1]["send_id"], status="sent", attempts=1, provider_message_id="msg_2"),
    ]
    store.reconcile_batch(batch_id, good_updates)
    assert store.campaign_stats("launch") == {"sent": 2}
    assert store.unfinished_batches("launch") == []


def test_recover_stale_sending_jobs_skips_durable_batch_items(tmp_path):
    from datetime import datetime, timedelta, timezone

    store = _store(tmp_path)
    r1, _ = store.upsert_recipient(Recipient(email="durable@example.com"))
    r2, _ = store.upsert_recipient(Recipient(email="legacy@example.com"))
    store.enqueue(r1, "launch")
    store.enqueue(r2, "launch")

    # durable job claimed via claim_batch -> attached to send_batches
    batch = store.claim_batch("launch", limit=1)
    durable_send_id = batch[0]["send_id"]

    # legacy job manually set to sending without send_batches entry
    with store._connect() as conn:
        row = conn.execute("SELECT id FROM campaign_sends WHERE recipient_id=?", (r2,)).fetchone()
        legacy_send_id = row["id"]
        old_time = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
        conn.execute("UPDATE campaign_sends SET status='sending', updated_at=? WHERE id IN (?, ?)",
                     (old_time, durable_send_id, legacy_send_id))

    # recover_stale_sending_jobs should ONLY recover the legacy job
    recovered_count = store.recover_stale_sending_jobs("launch", older_than_seconds=300)
    assert recovered_count == 1

    with store._connect() as conn:
        durable_status = conn.execute("SELECT status FROM campaign_sends WHERE id=?", (durable_send_id,)).fetchone()["status"]
        legacy_status = conn.execute("SELECT status FROM campaign_sends WHERE id=?", (legacy_send_id,)).fetchone()["status"]

    assert durable_status == "sending"
    assert legacy_status == "pending"


def test_mark_sent_preserves_existing_provider_message_id(tmp_path):
    store = _store(tmp_path)
    rid, _ = store.upsert_recipient(Recipient(email="msg@example.com"))
    store.enqueue(rid, "launch")
    send_id = store.claim_batch("launch", limit=1)[0]["send_id"]

    store.mark_sent(send_id, provider_message_id="initial_msg_id")
    with store._connect() as conn:
        mid1 = conn.execute("SELECT provider_message_id FROM campaign_sends WHERE id=?", (send_id,)).fetchone()["provider_message_id"]
    assert mid1 == "initial_msg_id"

    # Calling mark_sent with None should not wipe out existing ID
    store.mark_sent(send_id, provider_message_id=None)
    with store._connect() as conn:
        mid2 = conn.execute("SELECT provider_message_id FROM campaign_sends WHERE id=?", (send_id,)).fetchone()["provider_message_id"]
    assert mid2 == "initial_msg_id"
