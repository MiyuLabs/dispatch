from __future__ import annotations

from typing import Any, Optional

import pytest

from dispatch.providers.base import (
    BatchItemResult,
    EmailJob,
    EmailProvider,
    PermanentProviderError,
    SendResult,
    TransientProviderError,
)
from tests.fakes import (
    CONTRACT_PERM_FAIL_EMAIL,
    CONTRACT_RETRY_AFTER_EMAIL,
    CONTRACT_RETRY_AFTER_SECONDS,
    CONTRACT_SUCCESS_EMAIL,
    CONTRACT_TRANSIENT_FAIL_EMAIL,
)


class EmailProviderContract:
    """Reusable specification contract for any EmailProvider implementation.

    This suite verifies that an EmailProvider conforms to the requirements expected
    by the Dispatch campaign engine:
      1. Single delivery success returns a SendResult with a non-empty provider message ID.
      2. Single delivery permanent failure raises PermanentProviderError.
      3. Single delivery transient failure raises TransientProviderError.
      4. Single delivery accepts idempotency keys and optional sender fields without error.
      5. Empty batch calls return an empty list without error or network calls.
      6. Batch delivery preserves opaque `ref` identifiers and order across results.
      7. Batch delivery reports per-item success with non-empty message IDs.
      8. Batch delivery isolates permanent failures (permanent=True) without failing good sends.
      9. Batch delivery isolates transient failures (permanent=False) without failing good sends.
      10. Batch delivery propagates retry-after hints when available.
      11. Batch delivery accepts idempotency key prefixes and optional sender metadata.

    How to use for a new provider:
        class TestSesProviderContract(EmailProviderContract):
            __test__ = True

            @pytest.fixture
            def provider(self):
                return SesProvider(...)
    """

    __test__ = False

    @pytest.fixture
    def provider(self) -> EmailProvider:
        """Provider fixture that subclasses MUST override."""
        raise NotImplementedError("Subclasses of EmailProviderContract must define a `provider` fixture.")

    @pytest.fixture
    def default_from_email(self) -> str:
        """Default sender address for contract tests."""
        return "sender@contract.test"

    @pytest.fixture
    def supports_retry_after(self) -> bool:
        """Whether the provider supports extracting retry-after seconds.

        Subclasses may override to False if the underlying provider does not
        support retry-after metadata.
        """
        return True

    # ---- Single send contract -----------------------------------------------

    def test_send_success(self, provider: EmailProvider, default_from_email: str) -> None:
        """Single send with valid recipient succeeds and returns non-empty provider_message_id."""
        result = provider.send(
            to=CONTRACT_SUCCESS_EMAIL,
            subject="Contract Test Subject",
            html="<p>Contract Test HTML</p>",
            text="Contract Test Text",
            from_email=default_from_email,
        )
        assert isinstance(result, SendResult)
        assert isinstance(result.provider_message_id, str)
        assert len(result.provider_message_id.strip()) > 0

    def test_send_permanent_failure(self, provider: EmailProvider, default_from_email: str) -> None:
        """Single send with unrecoverable address raises PermanentProviderError."""
        with pytest.raises(PermanentProviderError) as exc_info:
            provider.send(
                to=CONTRACT_PERM_FAIL_EMAIL,
                subject="Contract Test Subject",
                html="<p>Contract Test HTML</p>",
                text="Contract Test Text",
                from_email=default_from_email,
            )
        assert len(str(exc_info.value).strip()) > 0

    def test_send_transient_failure(self, provider: EmailProvider, default_from_email: str) -> None:
        """Single send with retryable error raises TransientProviderError."""
        with pytest.raises(TransientProviderError) as exc_info:
            provider.send(
                to=CONTRACT_TRANSIENT_FAIL_EMAIL,
                subject="Contract Test Subject",
                html="<p>Contract Test HTML</p>",
                text="Contract Test Text",
                from_email=default_from_email,
            )
        assert len(str(exc_info.value).strip()) > 0

    def test_send_accepts_idempotency_key(self, provider: EmailProvider, default_from_email: str) -> None:
        """Single send accepts an idempotency_key parameter without raising unexpected errors."""
        result = provider.send(
            to=CONTRACT_SUCCESS_EMAIL,
            subject="Idempotency Test",
            html="<p>Idempotency</p>",
            text="Idempotency",
            from_email=default_from_email,
            idempotency_key="contract-idemp-key-xyz",
        )
        assert isinstance(result, SendResult)
        assert len(result.provider_message_id.strip()) > 0

    def test_send_accepts_optional_sender_fields(self, provider: EmailProvider, default_from_email: str) -> None:
        """Single send accepts from_name and reply_to parameters."""
        result = provider.send(
            to=CONTRACT_SUCCESS_EMAIL,
            subject="Optional Fields",
            html="<p>Optional</p>",
            text="Optional",
            from_email=default_from_email,
            from_name="Dispatch Contract",
            reply_to="reply@contract.test",
        )
        assert isinstance(result, SendResult)
        assert len(result.provider_message_id.strip()) > 0

    # ---- Batch send contract ------------------------------------------------

    def test_send_batch_empty(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send with an empty job list returns an empty list without error."""
        results = provider.send_batch([], from_email=default_from_email)
        assert results == []

    def test_send_batch_single_item(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send with a single valid job returns a successful BatchItemResult."""
        job = EmailJob(
            ref="job-101",
            to=CONTRACT_SUCCESS_EMAIL,
            subject="Single Batch Subject",
            html="<p>Single Batch</p>",
            text="Single Batch",
        )
        results = provider.send_batch([job], from_email=default_from_email)

        assert len(results) == 1
        r = results[0]
        assert isinstance(r, BatchItemResult)
        assert r.ref == "job-101"
        assert r.ok is True
        assert isinstance(r.message_id, str)
        assert len(r.message_id.strip()) > 0
        assert r.error is None
        assert r.permanent is None or r.permanent is False

    def test_send_batch_multiple_success(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send with multiple successful jobs returns success for all items in order."""
        jobs = [
            EmailJob(
                ref=i,
                to=f"user-{i}@contract.test",
                subject=f"Batch Subject {i}",
                html=f"<p>Batch {i}</p>",
                text=f"Batch {i}",
            )
            for i in range(3)
        ]
        results = provider.send_batch(jobs, from_email=default_from_email)

        assert len(results) == 3
        assert [r.ref for r in results] == [0, 1, 2]
        assert all(r.ok is True for r in results)
        assert all(isinstance(r.message_id, str) and len(r.message_id.strip()) > 0 for r in results)
        assert all(r.error is None for r in results)

    def test_send_batch_preserves_opaque_refs(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send preserves disparate opaque ref types (int, string, tuple)."""
        refs: list[Any] = [42, "opaque-uuid-str", ("composite", 7)]
        jobs = [
            EmailJob(
                ref=ref_val,
                to=f"recipient-{i}@contract.test",
                subject="Opaque Ref Test",
                html="<p>Ref Test</p>",
                text="Ref Test",
            )
            for i, ref_val in enumerate(refs)
        ]
        results = provider.send_batch(jobs, from_email=default_from_email)

        assert [r.ref for r in results] == refs

    def test_send_batch_permanent_failure(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send with permanently failing recipient returns ok=False and permanent=True."""
        job = EmailJob(
            ref="perm-ref",
            to=CONTRACT_PERM_FAIL_EMAIL,
            subject="Permanent Failure",
            html="<p>Fail</p>",
            text="Fail",
        )
        results = provider.send_batch([job], from_email=default_from_email)

        assert len(results) == 1
        r = results[0]
        assert r.ref == "perm-ref"
        assert r.ok is False
        assert r.permanent is True
        assert isinstance(r.error, str)
        assert len(r.error.strip()) > 0

    def test_send_batch_transient_failure(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send with transient failure returns ok=False and permanent=False."""
        job = EmailJob(
            ref="trans-ref",
            to=CONTRACT_TRANSIENT_FAIL_EMAIL,
            subject="Transient Failure",
            html="<p>Fail</p>",
            text="Fail",
        )
        results = provider.send_batch([job], from_email=default_from_email)

        assert len(results) == 1
        r = results[0]
        assert r.ref == "trans-ref"
        assert r.ok is False
        assert r.permanent is False
        assert isinstance(r.error, str)
        assert len(r.error.strip()) > 0

    def test_send_batch_partial_failures_mixed(self, provider: EmailProvider, default_from_email: str) -> None:
        """Partial batch failures are reported independently without failing good sends."""
        jobs = [
            EmailJob(ref="job-good-1", to=CONTRACT_SUCCESS_EMAIL, subject="S1", html="<p>1</p>", text="1"),
            EmailJob(ref="job-perm-2", to=CONTRACT_PERM_FAIL_EMAIL, subject="S2", html="<p>2</p>", text="2"),
            EmailJob(ref="job-trans-3", to=CONTRACT_TRANSIENT_FAIL_EMAIL, subject="S3", html="<p>3</p>", text="3"),
            EmailJob(ref="job-good-4", to="user4@contract.test", subject="S4", html="<p>4</p>", text="4"),
        ]
        results = provider.send_batch(jobs, from_email=default_from_email)

        assert len(results) == 4
        by_ref = {r.ref: r for r in results}

        assert by_ref["job-good-1"].ok is True
        assert isinstance(by_ref["job-good-1"].message_id, str)
        assert len(by_ref["job-good-1"].message_id.strip()) > 0
        assert by_ref["job-good-1"].error is None

        assert by_ref["job-perm-2"].ok is False
        assert by_ref["job-perm-2"].permanent is True
        assert isinstance(by_ref["job-perm-2"].error, str)
        assert len(by_ref["job-perm-2"].error.strip()) > 0

        assert by_ref["job-trans-3"].ok is False
        assert by_ref["job-trans-3"].permanent is False
        assert isinstance(by_ref["job-trans-3"].error, str)
        assert len(by_ref["job-trans-3"].error.strip()) > 0

        assert by_ref["job-good-4"].ok is True
        assert isinstance(by_ref["job-good-4"].message_id, str)
        assert len(by_ref["job-good-4"].message_id.strip()) > 0
        assert by_ref["job-good-4"].error is None

    def test_send_batch_retry_after(
        self,
        provider: EmailProvider,
        default_from_email: str,
        supports_retry_after: bool,
    ) -> None:
        """Batch send with rate limiting includes retry_after_seconds hint when supported."""
        job = EmailJob(
            ref="rate-ref",
            to=CONTRACT_RETRY_AFTER_EMAIL,
            subject="Rate Limited",
            html="<p>Rate</p>",
            text="Rate",
        )
        results = provider.send_batch([job], from_email=default_from_email)

        assert len(results) == 1
        r = results[0]
        assert r.ref == "rate-ref"
        assert r.ok is False
        assert r.permanent is False
        if supports_retry_after:
            assert r.retry_after_seconds is not None
            assert isinstance(r.retry_after_seconds, (int, float))
            assert r.retry_after_seconds > 0
        else:
            assert r.retry_after_seconds is None

    def test_send_batch_idempotency_prefix(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send accepts idempotency_key_prefix parameter."""
        job = EmailJob(
            ref="idemp-batch-ref",
            to=CONTRACT_SUCCESS_EMAIL,
            subject="Batch Idempotency",
            html="<p>Idemp</p>",
            text="Idemp",
        )
        results = provider.send_batch(
            [job],
            from_email=default_from_email,
            idempotency_key_prefix="campaign:test-contract:batch-0",
        )
        assert len(results) == 1
        assert results[0].ok is True

    def test_send_batch_optional_sender_fields(self, provider: EmailProvider, default_from_email: str) -> None:
        """Batch send accepts from_name and reply_to parameters."""
        job = EmailJob(
            ref="opt-batch-ref",
            to=CONTRACT_SUCCESS_EMAIL,
            subject="Batch Optional",
            html="<p>Opt</p>",
            text="Opt",
        )
        results = provider.send_batch(
            [job],
            from_email=default_from_email,
            from_name="Dispatch Batch Sender",
            reply_to="batch-reply@contract.test",
        )
        assert len(results) == 1
        assert results[0].ok is True
