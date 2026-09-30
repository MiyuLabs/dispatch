from __future__ import annotations

from typing import Optional
from unittest.mock import patch

import pytest

from dispatch.providers.base import (
    EmailJob,
    EmailProvider,
    PermanentProviderError,
    SendResult,
    TransientProviderError,
)
from dispatch.providers.resend_provider import ResendProvider
from tests.fakes import (
    CONTRACT_PERM_FAIL_EMAIL,
    CONTRACT_RETRY_AFTER_EMAIL,
    CONTRACT_RETRY_AFTER_SECONDS,
    CONTRACT_SUCCESS_EMAIL,
    CONTRACT_TRANSIENT_FAIL_EMAIL,
    DefaultBatchProvider,
    FakeEmailProvider,
)
from tests.provider_contract import EmailProviderContract


# =============================================================================
# 1. FakeEmailProvider Contract Tests
# =============================================================================


class TestFakeEmailProvider(EmailProviderContract):
    """Verifies that the in-memory FakeEmailProvider fully adheres to the provider contract."""

    __test__ = True

    @pytest.fixture
    def provider(self) -> FakeEmailProvider:
        return FakeEmailProvider()


# =============================================================================
# 2. DefaultBatchProvider (Base Class send_batch) Contract Tests
# =============================================================================


class TestDefaultEmailProvider(EmailProviderContract):
    """Verifies that providers relying on the default EmailProvider.send_batch() loop

    satisfy the full provider contract.
    """

    __test__ = True

    @pytest.fixture
    def provider(self) -> DefaultBatchProvider:
        return DefaultBatchProvider()


def test_abstract_email_provider_cannot_be_instantiated():
    """Verify that EmailProvider cannot be instantiated without implementing send()."""

    class IncompleteProvider(EmailProvider):
        pass

    with pytest.raises(TypeError):
        IncompleteProvider()  # type: ignore[abstract]


def test_default_send_batch_formats_idempotency_keys_per_index():
    """Verify default EmailProvider.send_batch formats idempotency keys as prefix:index."""
    provider = FakeEmailProvider()
    jobs = [
        EmailJob(ref=1, to=CONTRACT_SUCCESS_EMAIL, subject="S", html="<p/>", text="t"),
        EmailJob(ref=2, to=CONTRACT_SUCCESS_EMAIL, subject="S", html="<p/>", text="t"),
    ]
    # Call base send_batch explicitly to verify default idempotency key generation
    EmailProvider.send_batch(
        provider,
        jobs,
        from_email="sender@contract.test",
        idempotency_key_prefix="batch-test",
    )
    assert provider.recorded_idempotency_keys == ["batch-test:0", "batch-test:1"]


# =============================================================================
# 3. ResendProvider Contract Tests (Mocked Offline)
# =============================================================================


class _ResendFakeError(Exception):
    """Simulated Resend SDK exception with status code and optional response headers."""

    def __init__(self, message: str, status_code: int, headers: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.headers = headers or {}


def _mock_resend_emails_send(params: dict, options: Optional[dict] = None) -> dict:
    """Mock for resend.Emails.send that routes responses according to contract addresses."""
    to_list = params.get("to", [])
    to = to_list[0] if to_list else ""

    if to == CONTRACT_PERM_FAIL_EMAIL or "perm-fail" in to:
        raise _ResendFakeError(f"Simulated Resend 400 validation error for {to}", status_code=400)
    if to == CONTRACT_RETRY_AFTER_EMAIL or "retry-after" in to:
        raise _ResendFakeError(
            f"Simulated Resend 429 rate limit for {to}",
            status_code=429,
            headers={"retry-after": str(CONTRACT_RETRY_AFTER_SECONDS)},
        )
    if to == CONTRACT_TRANSIENT_FAIL_EMAIL or "transient-fail" in to:
        raise _ResendFakeError(f"Simulated Resend 500 server error for {to}", status_code=500)

    return {"id": f"resend-single-{to}"}


def _mock_resend_batch_send(payload: list[dict], options: Optional[dict] = None) -> dict:
    """Mock for resend.Batch.send supporting permissive mode, fallback triggers, and rate limits."""
    # 1. Rate limiting
    if any(
        CONTRACT_RETRY_AFTER_EMAIL in item.get("to", [])
        or any("retry-after" in t for t in item.get("to", []))
        for item in payload
    ):
        raise _ResendFakeError(
            "Simulated Resend 429 rate limit on batch endpoint",
            status_code=429,
            headers={"retry-after": str(CONTRACT_RETRY_AFTER_SECONDS)},
        )

    # 2. If all items are transient failures, simulate batch-level 500
    all_transient = all(
        CONTRACT_TRANSIENT_FAIL_EMAIL in item.get("to", [])
        or any("transient-fail" in t for t in item.get("to", []))
        for item in payload
    )
    if all_transient:
        raise _ResendFakeError("Simulated Resend 500 internal server error", status_code=500)

    # 3. If mixed batch contains transient failures, simulate 400 batch rejection
    # to exercise ResendProvider's fallback loop to individual send() calls.
    has_transient = any(
        CONTRACT_TRANSIENT_FAIL_EMAIL in item.get("to", [])
        or any("transient-fail" in t for t in item.get("to", []))
        for item in payload
    )
    if has_transient:
        raise _ResendFakeError("Simulated 400 batch validation failure", status_code=400)

    # 4. Otherwise simulate Resend permissive batch responses
    data = []
    errors = []
    for i, item in enumerate(payload):
        to = item.get("to", [""])[0]
        if to == CONTRACT_PERM_FAIL_EMAIL or "perm-fail" in to:
            errors.append({"index": i, "message": f"Simulated invalid address: {to}"})
        else:
            data.append({"id": f"resend-batch-{to}"})

    return {"data": data, "errors": errors}


class TestResendProviderContract(EmailProviderContract):
    """Verifies that ResendProvider conforms to the provider contract without network calls."""

    __test__ = True

    @pytest.fixture(autouse=True)
    def setup_resend_mocks(self):
        with patch("dispatch.providers.resend_provider.resend.Emails.send", side_effect=_mock_resend_emails_send), \
             patch("dispatch.providers.resend_provider.resend.Batch.send", side_effect=_mock_resend_batch_send):
            yield

    @pytest.fixture
    def provider(self) -> ResendProvider:
        # High rate limit token bucket to prevent sleeping during contract tests
        return ResendProvider(api_key="re_contract_dummy_key", rate_limit_per_second=1000.0)
