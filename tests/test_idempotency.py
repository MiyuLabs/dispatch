from unittest.mock import patch, MagicMock
import pytest

from dispatch.providers.base import (
    EmailJob,
    EmailProvider,
    SendResult,
    BatchItemResult,
    PermanentProviderError,
    TransientProviderError,
    recipient_idempotency_key,
)
from dispatch.providers.resend_provider import (
    ResendProvider,
    batch_request_idempotency_key,
    _batch_key,
)


class _CaptureSingleSendProvider(EmailProvider):
    def __init__(self):
        self.recorded_calls = []

    def send(self, **kwargs) -> SendResult:
        self.recorded_calls.append(kwargs)
        return SendResult(provider_message_id=f"msg-{kwargs.get('idempotency_key')}")


# ==============================================================================
# TEST 1 — deterministic key
# ==============================================================================

def test_deterministic_key():
    campaign = "launch"
    send_id = 101

    key1 = recipient_idempotency_key(campaign, send_id)
    key2 = recipient_idempotency_key(campaign, send_id)

    assert key1 == key2
    assert key1 == "dispatch:launch:send:101"


# ==============================================================================
# TEST 2 — different recipients
# ==============================================================================

def test_different_recipients():
    campaign = "launch"
    key101 = recipient_idempotency_key(campaign, 101)
    key102 = recipient_idempotency_key(campaign, 102)

    assert key101 != key102
    assert "101" in key101
    assert "102" in key102


# ==============================================================================
# TEST 3 — different campaigns
# ==============================================================================

def test_different_campaigns():
    send_id = 101
    key_a = recipient_idempotency_key("campaign-a", send_id)
    key_b = recipient_idempotency_key("campaign-b", send_id)

    assert key_a != key_b
    assert "campaign-a" in key_a
    assert "campaign-b" in key_b


# ==============================================================================
# TEST 4 — retry stability
# ==============================================================================

def test_retry_stability():
    campaign = "launch"
    send_id = 205

    # Simulate key generation on first attempt
    first_attempt_key = recipient_idempotency_key(campaign, send_id)

    # Simulate key generation on retry (e.g. after transient failure or restart)
    retry_attempt_key = recipient_idempotency_key(campaign, send_id)

    assert first_attempt_key == retry_attempt_key


# ==============================================================================
# TEST 5 — batch composition does not change logical key
# ==============================================================================

def test_batch_composition_does_not_change_logical_key():
    campaign = "launch"
    # Initial batch with 3 jobs
    jobs_initial = [
        EmailJob(ref=sid, to=f"{sid}@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, sid))
        for sid in [101, 102, 103]
    ]

    keys_initial = {j.ref: j.idempotency_key for j in jobs_initial}

    # Suppose 101 and 102 succeed, and 103 is retried in a new subset batch [103]
    jobs_retry = [
        EmailJob(ref=103, to="103@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, 103))
    ]

    assert jobs_retry[0].idempotency_key == keys_initial[103]


# ==============================================================================
# TEST 6 — ordering does not change logical keys
# ==============================================================================

def test_ordering_does_not_change_logical_keys():
    campaign = "launch"

    order_1 = [101, 102, 103]
    order_2 = [103, 101, 102]

    jobs_1 = [
        EmailJob(ref=sid, to=f"{sid}@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, sid))
        for sid in order_1
    ]

    jobs_2 = [
        EmailJob(ref=sid, to=f"{sid}@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, sid))
        for sid in order_2
    ]

    keys_by_ref_1 = {j.ref: j.idempotency_key for j in jobs_1}
    keys_by_ref_2 = {j.ref: j.idempotency_key for j in jobs_2}

    for sid in [101, 102, 103]:
        assert keys_by_ref_1[sid] == keys_by_ref_2[sid]


# ==============================================================================
# TEST 7 — single send uses deterministic key
# ==============================================================================

def test_single_send_uses_deterministic_key():
    provider = ResendProvider(api_key="re_test", rate_limit_per_second=1000)
    campaign = "launch"
    send_id = 101
    key = recipient_idempotency_key(campaign, send_id)

    with patch("dispatch.providers.resend_provider.resend.Emails.send") as mock_send:
        mock_send.return_value = {"id": "msg_first"}
        provider.send(
            to="user@example.com",
            subject="Hello",
            html="<p>Hi</p>",
            text="Hi",
            from_email="noreply@example.com",
            idempotency_key=key,
        )

        assert mock_send.call_count == 1
        args_1, kwargs_1 = mock_send.call_args
        opts_dict_1 = args_1[1] if len(args_1) > 1 else kwargs_1.get("options", {})
        assert opts_dict_1.get("idempotency_key") == "dispatch:launch:send:101"

        # Retry the same logical send
        mock_send.return_value = {"id": "msg_retry"}
        provider.send(
            to="user@example.com",
            subject="Hello",
            html="<p>Hi</p>",
            text="Hi",
            from_email="noreply@example.com",
            idempotency_key=key,
        )

        assert mock_send.call_count == 2
        args_2, kwargs_2 = mock_send.call_args
        opts_dict_2 = args_2[1] if len(args_2) > 1 else kwargs_2.get("options", {})
        assert opts_dict_2.get("idempotency_key") == "dispatch:launch:send:101"
        assert opts_dict_1.get("idempotency_key") == opts_dict_2.get("idempotency_key")


# ==============================================================================
# TEST 8 — default EmailProvider batch uses deterministic key independent of position
# ==============================================================================

def test_default_email_provider_batch_independent_of_position():
    provider = _CaptureSingleSendProvider()
    campaign = "launch"

    # Attempt 1: job 101 is at index 0, job 102 is at index 1
    jobs_attempt_1 = [
        EmailJob(ref=101, to="a@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, 101)),
        EmailJob(ref=102, to="b@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, 102)),
    ]
    provider.send_batch(jobs_attempt_1, from_email="hi@example.com")

    # Attempt 2: retry job 102 alone (now at index 0)
    jobs_attempt_2 = [
        EmailJob(ref=102, to="b@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, 102)),
    ]
    provider.send_batch(jobs_attempt_2, from_email="hi@example.com")

    # Check calls for job 102 across attempts
    call_job_102_attempt_1 = [c for c in provider.recorded_calls if c["to"] == "b@example.com"][0]
    call_job_102_attempt_2 = [c for c in provider.recorded_calls if c["to"] == "b@example.com"][1]

    assert call_job_102_attempt_1["idempotency_key"] == "dispatch:launch:send:102"
    assert call_job_102_attempt_2["idempotency_key"] == "dispatch:launch:send:102"
    assert call_job_102_attempt_1["idempotency_key"] == call_job_102_attempt_2["idempotency_key"]


# ==============================================================================
# TEST 9 — Resend batch key remains deterministic
# ==============================================================================

def test_resend_batch_key_remains_deterministic():
    campaign = "launch"
    prefix = f"dispatch:{campaign}"

    jobs_a = [
        EmailJob(ref=101, to="a@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, 101)),
        EmailJob(ref=102, to="b@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, 102)),
    ]
    jobs_a_reordered = [jobs_a[1], jobs_a[0]]

    # Identical membership (even if reordered) produces identical transport batch key
    batch_key_1 = batch_request_idempotency_key(prefix, jobs_a)
    batch_key_2 = batch_request_idempotency_key(prefix, jobs_a_reordered)
    assert batch_key_1 == batch_key_2

    # Different membership (e.g. subset retry [102]) produces different transport batch key
    jobs_subset = [jobs_a[1]]
    batch_key_subset = batch_request_idempotency_key(prefix, jobs_subset)
    assert batch_key_subset != batch_key_1


# ==============================================================================
# TEST 10 — partial batch retry
# ==============================================================================

def test_partial_batch_retry_preserves_recipient_key():
    campaign = "launch"

    # First request: [101, 102, 103]
    jobs_all = [
        EmailJob(ref=sid, to=f"{sid}@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, sid))
        for sid in [101, 102, 103]
    ]
    key_103_first = jobs_all[2].idempotency_key

    # Second request: retry only [103]
    jobs_retry = [
        EmailJob(ref=103, to="103@example.com", subject="s", html="h", text="t",
                 idempotency_key=recipient_idempotency_key(campaign, 103))
    ]
    key_103_retry = jobs_retry[0].idempotency_key

    assert key_103_first == key_103_retry
    assert key_103_retry == "dispatch:launch:send:103"


# ==============================================================================
# TEST 11 — 409 conflict handling
# ==============================================================================

class _Fake409Error(Exception):
    def __init__(self, message: str):
        self.status_code = 409
        super().__init__(message)


def test_409_concurrent_is_transient_and_conflict_is_permanent():
    provider = ResendProvider(api_key="re_test", rate_limit_per_second=1000)

    # 1. Concurrent request 409 -> TransientProviderError (will retry with backoff)
    with patch("dispatch.providers.resend_provider.resend.Emails.send") as mock_send:
        mock_send.side_effect = _Fake409Error("Concurrent idempotent request: request in progress")
        with pytest.raises(TransientProviderError):
            provider.send(
                to="a@example.com", subject="s", html="h", text="t", from_email="x@example.com",
                idempotency_key="dispatch:launch:send:1",
            )

    # 2. Parameter mismatch 409 -> PermanentProviderError (payload mismatch, don't blindly loop)
    with patch("dispatch.providers.resend_provider.resend.Emails.send") as mock_send:
        mock_send.side_effect = _Fake409Error("Idempotency key conflict: parameters do not match")
        with pytest.raises(PermanentProviderError):
            provider.send(
                to="a@example.com", subject="s", html="h", text="t", from_email="x@example.com",
                idempotency_key="dispatch:launch:send:1",
            )

    # 3. Batch 409 handling
    job = EmailJob(ref=1, to="a@example.com", subject="s", html="h", text="t",
                   idempotency_key="dispatch:launch:send:1")
    with patch("dispatch.providers.resend_provider.resend.Batch.send") as mock_batch:
        mock_batch.side_effect = _Fake409Error("Idempotency key conflict")
        results = provider.send_batch([job], from_email="x@example.com")
        assert len(results) == 1
        assert results[0].ok is False
        assert results[0].permanent is True


# ==============================================================================
# TEST 12 — no random key generation
# ==============================================================================

def test_no_random_key_generation():
    # 100 repeated generations produce the identical result with no randomness
    keys = {recipient_idempotency_key("launch", 42) for _ in range(100)}
    assert len(keys) == 1
    assert list(keys)[0] == "dispatch:launch:send:42"
