from __future__ import annotations

from typing import Optional

from dispatch.providers.base import (
    BatchItemResult,
    EmailJob,
    EmailProvider,
    PermanentProviderError,
    SendResult,
    TransientProviderError,
)

# Standard recipient emails recognized by contract test fixtures to trigger
# specific provider responses.
CONTRACT_SUCCESS_EMAIL = "success@contract.test"
CONTRACT_PERM_FAIL_EMAIL = "perm-fail@contract.test"
CONTRACT_TRANSIENT_FAIL_EMAIL = "transient-fail@contract.test"
CONTRACT_RETRY_AFTER_EMAIL = "retry-after@contract.test"
CONTRACT_RETRY_AFTER_SECONDS = 15.5


class FakeEmailProvider(EmailProvider):
    """Deterministic in-memory EmailProvider for contract and unit testing.

    Simulates success, permanent failure, transient failure, and retry-after hints
    based on the recipient address without network requests or external SDKs.
    """

    def __init__(self):
        self.sent_single_calls: list[dict] = []
        self.sent_batch_calls: list[dict] = []
        self.recorded_idempotency_keys: list[Optional[str]] = []

    def send(
        self,
        *,
        to: str,
        subject: str,
        html: str,
        text: str,
        from_email: str,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> SendResult:
        self.sent_single_calls.append({
            "to": to,
            "subject": subject,
            "html": html,
            "text": text,
            "from_email": from_email,
            "from_name": from_name,
            "reply_to": reply_to,
            "idempotency_key": idempotency_key,
        })
        self.recorded_idempotency_keys.append(idempotency_key)

        if to == CONTRACT_PERM_FAIL_EMAIL or "perm-fail" in to:
            raise PermanentProviderError(f"Simulated permanent failure for {to}")
        if to == CONTRACT_TRANSIENT_FAIL_EMAIL or "transient-fail" in to:
            raise TransientProviderError(f"Simulated transient failure for {to}")
        if to == CONTRACT_RETRY_AFTER_EMAIL or "retry-after" in to:
            raise TransientProviderError(
                f"Simulated rate limit for {to}",
                retry_after_seconds=CONTRACT_RETRY_AFTER_SECONDS,
            )

        return SendResult(provider_message_id=f"fake-msg-{to}")

    def send_batch(
        self,
        jobs: list[EmailJob],
        *,
        from_email: str,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        idempotency_key_prefix: Optional[str] = None,
    ) -> list[BatchItemResult]:
        self.sent_batch_calls.append({
            "jobs": list(jobs),
            "from_email": from_email,
            "from_name": from_name,
            "reply_to": reply_to,
            "idempotency_key_prefix": idempotency_key_prefix,
        })

        results: list[BatchItemResult] = []
        for job in jobs:
            to = job.to
            if to == CONTRACT_PERM_FAIL_EMAIL or "perm-fail" in to:
                results.append(BatchItemResult(
                    ref=job.ref,
                    ok=False,
                    error=f"Simulated permanent failure for {to}",
                    permanent=True,
                ))
            elif to == CONTRACT_RETRY_AFTER_EMAIL or "retry-after" in to:
                results.append(BatchItemResult(
                    ref=job.ref,
                    ok=False,
                    error=f"Simulated rate limit for {to}",
                    permanent=False,
                    retry_after_seconds=CONTRACT_RETRY_AFTER_SECONDS,
                ))
            elif to == CONTRACT_TRANSIENT_FAIL_EMAIL or "transient-fail" in to:
                results.append(BatchItemResult(
                    ref=job.ref,
                    ok=False,
                    error=f"Simulated transient failure for {to}",
                    permanent=False,
                ))
            else:
                results.append(BatchItemResult(
                    ref=job.ref,
                    ok=True,
                    message_id=f"fake-batch-msg-{job.ref}",
                ))
        return results


class DefaultBatchProvider(EmailProvider):
    """Minimal EmailProvider that implements ONLY send().

    Used to verify that the base EmailProvider.send_batch() implementation
    satisfies the provider contract when subclasses rely on default loop behavior.
    """

    def __init__(self):
        self.sent_calls: list[dict] = []

    def send(
        self,
        *,
        to: str,
        subject: str,
        html: str,
        text: str,
        from_email: str,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> SendResult:
        self.sent_calls.append({
            "to": to,
            "subject": subject,
            "html": html,
            "text": text,
            "from_email": from_email,
            "from_name": from_name,
            "reply_to": reply_to,
            "idempotency_key": idempotency_key,
        })

        if to == CONTRACT_PERM_FAIL_EMAIL or "perm-fail" in to:
            raise PermanentProviderError(f"Simulated permanent failure for {to}")
        if to == CONTRACT_TRANSIENT_FAIL_EMAIL or "transient-fail" in to:
            raise TransientProviderError(f"Simulated transient failure for {to}")
        if to == CONTRACT_RETRY_AFTER_EMAIL or "retry-after" in to:
            raise TransientProviderError(
                f"Simulated rate limit for {to}",
                retry_after_seconds=CONTRACT_RETRY_AFTER_SECONDS,
            )

        return SendResult(provider_message_id=f"default-msg-{to}")
