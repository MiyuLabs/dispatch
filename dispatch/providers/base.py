from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


class TransientProviderError(Exception):
    """A failure worth retrying: network blip, 5xx, or 429 rate limit."""


class PermanentProviderError(Exception):
    """A failure that will never succeed on retry: bad address, invalid
    request, auth failure, etc. The engine marks these jobs as failed
    without burning through retry attempts."""


@dataclass(frozen=True)
class SendResult:
    provider_message_id: str


@dataclass(frozen=True)
class EmailJob:
    """Everything a provider needs to send one message, tagged with the
    caller's own id so results can be matched back up. `ref` is opaque to
    the provider — the engine passes the campaign_sends row id."""

    ref: object
    to: str
    subject: str
    html: str
    text: str


@dataclass(frozen=True)
class BatchItemResult:
    """One job's outcome within a `send_batch()` call. Never raises —
    batch calls report success/failure per item as data, since one bad
    address shouldn't stop the engine from learning about the other 99."""

    ref: object
    ok: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    # Only meaningful when ok=False. True = don't retry (dead), False = worth
    # retrying with backoff. None ("unknown cause") is treated as retryable —
    # the safer, non-destructive default.
    permanent: Optional[bool] = None
    # Best-effort `retry-after` hint (seconds) from a 429 response, if the
    # provider could extract one. The engine uses this as a floor under its
    # own computed exponential backoff rather than replacing it outright.
    retry_after_seconds: Optional[float] = None


class EmailProvider(ABC):
    """A thing that can send email. Swap Resend for SES, Postmark, SMTP,
    etc. by adding a subclass here.

    `send()` is the only required method — a single message, raises on
    failure. `send_batch()` has a working default (loop over `send()`) so
    every provider gets *a* batch API for free; a provider with a real
    bulk endpoint (Resend does) overrides it so one HTTP call covers many
    recipients instead of one call each. The engine always calls
    `send_batch()` — this is the one place batching logic lives.
    """

    @abstractmethod
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
        raise NotImplementedError

    def send_batch(
        self,
        jobs: list[EmailJob],
        *,
        from_email: str,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        idempotency_key_prefix: Optional[str] = None,
    ) -> list[BatchItemResult]:
        """Naive default: one `send()` call per job. Correct for any
        provider, just not efficient — override this for a provider with a
        real bulk endpoint."""
        results = []
        for i, job in enumerate(jobs):
            key = f"{idempotency_key_prefix}:{i}" if idempotency_key_prefix else None
            try:
                result = self.send(
                    to=job.to, subject=job.subject, html=job.html, text=job.text,
                    from_email=from_email, from_name=from_name, reply_to=reply_to,
                    idempotency_key=key,
                )
            except PermanentProviderError as e:
                results.append(BatchItemResult(ref=job.ref, ok=False, error=str(e), permanent=True))
            except TransientProviderError as e:
                results.append(BatchItemResult(ref=job.ref, ok=False, error=str(e), permanent=False))
            else:
                results.append(BatchItemResult(ref=job.ref, ok=True, message_id=result.provider_message_id))
        return results
