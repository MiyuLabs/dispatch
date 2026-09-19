from unittest.mock import patch, MagicMock

from dispatch.providers.base import EmailJob
from dispatch.providers.resend_provider import ResendProvider


def _provider():
    # Rate limiter set high so tests don't actually sleep.
    return ResendProvider(api_key="re_test", rate_limit_per_second=1000, max_batch_size=2)


def test_send_batch_success_maps_results_in_order():
    jobs = [
        EmailJob(ref=1, to="a@example.com", subject="s", html="<p>1</p>", text="1"),
        EmailJob(ref=2, to="b@example.com", subject="s", html="<p>2</p>", text="2"),
    ]
    provider = _provider()
    with patch("dispatch.providers.resend_provider.resend.Batch.send") as mock_send:
        mock_send.return_value = {"data": [{"id": "id-1"}, {"id": "id-2"}]}
        results = provider.send_batch(jobs, from_email="hi@miyulabs.in")

    assert [r.ref for r in results] == [1, 2]
    assert all(r.ok for r in results)
    assert results[0].message_id == "id-1"
    assert results[1].message_id == "id-2"
    mock_send.assert_called_once()  # 2 jobs, max_batch_size=2 -> one chunk


def test_send_batch_chunks_by_max_batch_size():
    jobs = [
        EmailJob(ref=i, to=f"{i}@example.com", subject="s", html="<p/>", text="t")
        for i in range(5)
    ]
    provider = _provider()  # max_batch_size=2 -> 3 chunks (2,2,1)
    with patch("dispatch.providers.resend_provider.resend.Batch.send") as mock_send:
        mock_send.side_effect = [
            {"data": [{"id": "a"}, {"id": "b"}]},
            {"data": [{"id": "c"}, {"id": "d"}]},
            {"data": [{"id": "e"}]},
        ]
        results = provider.send_batch(jobs, from_email="hi@miyulabs.in")

    assert mock_send.call_count == 3
    assert [r.ref for r in results] == [0, 1, 2, 3, 4]
    assert all(r.ok for r in results)


class _FakeError(Exception):
    def __init__(self, status_code):
        self.status_code = status_code
        super().__init__(f"error {status_code}")


def test_validation_error_falls_back_to_individual_sends():
    jobs = [
        EmailJob(ref=1, to="good@example.com", subject="s", html="<p/>", text="t"),
        EmailJob(ref=2, to="bad-address", subject="s", html="<p/>", text="t"),
    ]
    provider = _provider()
    with patch("dispatch.providers.resend_provider.resend.Batch.send") as mock_batch, \
         patch("dispatch.providers.resend_provider.resend.Emails.send") as mock_single:
        mock_batch.side_effect = _FakeError(400)  # whole batch rejected
        # Individual fallback: first job succeeds, second is permanently invalid
        mock_single.side_effect = [
            {"id": "ok-1"},
            _FakeError(400),
        ]
        results = provider.send_batch(jobs, from_email="hi@miyulabs.in")

    assert mock_batch.call_count == 1
    assert mock_single.call_count == 2
    by_ref = {r.ref: r for r in results}
    assert by_ref[1].ok is True
    assert by_ref[2].ok is False
    assert by_ref[2].permanent is True


def test_rate_limit_error_retries_whole_chunk_without_fallback():
    jobs = [
        EmailJob(ref=1, to="a@example.com", subject="s", html="<p/>", text="t"),
        EmailJob(ref=2, to="b@example.com", subject="s", html="<p/>", text="t"),
    ]
    provider = _provider()
    with patch("dispatch.providers.resend_provider.resend.Batch.send") as mock_batch, \
         patch("dispatch.providers.resend_provider.resend.Emails.send") as mock_single:
        mock_batch.side_effect = _FakeError(429)
        results = provider.send_batch(jobs, from_email="hi@miyulabs.in")

    mock_single.assert_not_called()  # no fallback for account/rate-limit errors
    assert all(not r.ok and not r.permanent for r in results)


def test_permissive_batch_validation_handles_partial_success_in_single_call():
    jobs = [
        EmailJob(ref=1, to="good@example.com", subject="s", html="<p/>", text="t"),
        EmailJob(ref=2, to="bad-email", subject="s", html="<p/>", text="t"),
    ]
    provider = _provider()
    with patch("dispatch.providers.resend_provider.resend.Batch.send") as mock_batch, \
         patch("dispatch.providers.resend_provider.resend.Emails.send") as mock_single:
        # Permissive response: batch succeeded, data has the good email, errors has the bad email by index
        mock_batch.return_value = {
            "data": [{"id": "id-good"}],
            "errors": [{"index": 1, "message": "The email address is invalid."}],
        }
        results = provider.send_batch(jobs, from_email="hi@miyulabs.in")

    mock_batch.assert_called_once()
    mock_single.assert_not_called()  # Native permissive handling requires NO individual call loops!
    by_ref = {r.ref: r for r in results}
    assert by_ref[1].ok is True
    assert by_ref[1].message_id == "id-good"
    assert by_ref[2].ok is False
    assert by_ref[2].permanent is True
    assert "invalid" in by_ref[2].error


def test_retry_after_extraction_from_exc_headers():
    from dispatch.providers.resend_provider import _retry_after_of

    class _HeaderError(Exception):
        def __init__(self, headers):
            self.headers = headers

    err = _HeaderError({"retry-after": "15.5"})
    assert _retry_after_of(err) == 15.5

