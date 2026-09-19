from __future__ import annotations

import hashlib
import logging
from typing import Optional

import resend

from .base import (
    BatchItemResult,
    EmailJob,
    EmailProvider,
    PermanentProviderError,
    SendResult,
    TransientProviderError,
)
from ..ratelimit import TokenBucket

logger = logging.getLogger(__name__)

# Status codes documented at https://resend.com/docs/api-reference/errors
# where the response body is *about the request's data* — plausibly caused
# by one bad recipient in a batch — versus everything else (auth, quota,
# rate limit, 5xx), which affects the whole account/request equally and
# isn't something splitting the batch would fix.
_VALIDATION_STATUS_CODES = {400, 422}
# Single-send permanent set is wider: for exactly one recipient, auth/domain
# problems are just as "don't bother retrying this send" as bad data.
_SINGLE_SEND_PERMANENT_STATUS_CODES = {400, 401, 403, 404, 422}


def _status_of(exc: Exception) -> Optional[int]:
    status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _retry_after_of(exc: Exception) -> Optional[float]:
    """Best-effort extraction of a `retry-after` header from whatever the
    installed resend SDK version attaches to its exceptions. Returns None
    if we can't find one — callers fall back to computed backoff. This is
    the one place worth re-checking if you upgrade the `resend` package."""
    for obj in (exc, getattr(exc, "response", None), getattr(exc, "http_response", None)):
        if obj is None:
            continue
        headers = getattr(obj, "headers", None)
        if headers and isinstance(headers, dict):
            for key in ("retry-after", "Retry-After"):
                if key in headers:
                    try:
                        return float(headers[key])
                    except (TypeError, ValueError):
                        pass
    return None


def _batch_key(prefix: str, chunk: list[EmailJob]) -> str:
    """One idempotency key per exact set of recipients in this call. If the
    same chunk is retried unchanged, Resend recognizes it and won't
    double-send. If the chunk's membership changes (e.g. a smaller retry
    after some jobs already succeeded), the key changes too, as it must."""
    digest = hashlib.sha256(",".join(sorted(str(j.ref) for j in chunk)).encode()).hexdigest()[:24]
    return f"{prefix}:batch:{digest}"


class ResendProvider(EmailProvider):
    """Sends via Resend (https://resend.com).

    Owns its own rate limiter because rate limits are a property of the
    provider's API, not of the engine — a different provider (SES, SMTP)
    would have entirely different limits or none at all. Resend's default
    is 10 requests/sec **shared across the whole team**, so
    `rate_limit_per_second` should generally be set somewhat below 10 if
    anything else in the org might also be calling the API concurrently.

    `send_batch()` makes one HTTP call per up-to-100 recipients — that
    call counts as a single request against the rate limit no matter how
    many recipients it carries, which is the whole point of using it.
    With `batch_validation='permissive'`, Resend processes and delivers all
    valid emails in the batch, reporting any invalid entries in an `errors`
    array tagged by index. That allows partial success in a single API call
    without holding good addresses hostage or looping one-by-one.
    If a batch call fails outright (400/422 on an account level), we fall
    back to sending one-by-one as a safety net.
    """

    def __init__(self, api_key: str, rate_limit_per_second: float = 8.0, max_batch_size: int = 100):
        resend.api_key = api_key
        self._rate_limiter = TokenBucket(rate_limit_per_second)
        self._max_batch_size = max_batch_size

    # ---- single send ------------------------------------------------------

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
        sender = f"{from_name} <{from_email}>" if from_name else from_email
        params: dict = {"from": sender, "to": [to], "subject": subject, "html": html, "text": text}
        if reply_to:
            params["reply_to"] = reply_to
        options = {"idempotency_key": idempotency_key} if idempotency_key else {}

        self._rate_limiter.acquire()
        try:
            result = resend.Emails.send(params, options)
        except Exception as exc:
            status = _status_of(exc)
            if status in _SINGLE_SEND_PERMANENT_STATUS_CODES:
                raise PermanentProviderError(f"{type(exc).__name__}: {exc}") from exc
            raise TransientProviderError(f"{type(exc).__name__}: {exc}") from exc

        message_id = result.get("id", "") if isinstance(result, dict) else (getattr(result, "id", "") or "")
        return SendResult(provider_message_id=message_id)

    # ---- batch send ---------------------------------------------------------

    def send_batch(
        self,
        jobs: list[EmailJob],
        *,
        from_email: str,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        idempotency_key_prefix: Optional[str] = None,
    ) -> list[BatchItemResult]:
        results: list[BatchItemResult] = []
        for start in range(0, len(jobs), self._max_batch_size):
            chunk = jobs[start:start + self._max_batch_size]
            results.extend(self._send_one_chunk(chunk, from_email, from_name, reply_to, idempotency_key_prefix))
        return results

    def _send_one_chunk(
        self,
        chunk: list[EmailJob],
        from_email: str,
        from_name: Optional[str],
        reply_to: Optional[str],
        idempotency_key_prefix: Optional[str],
    ) -> list[BatchItemResult]:
        sender = f"{from_name} <{from_email}>" if from_name else from_email
        payload = []
        for job in chunk:
            item = {"from": sender, "to": [job.to], "subject": job.subject, "html": job.html, "text": job.text}
            if reply_to:
                item["reply_to"] = reply_to
            payload.append(item)

        options: dict = {"batch_validation": "permissive"}
        if idempotency_key_prefix:
            options["idempotency_key"] = _batch_key(idempotency_key_prefix, chunk)

        self._rate_limiter.acquire()  # one token for the WHOLE chunk, not per recipient
        try:
            result = resend.Batch.send(payload, options)
        except Exception as exc:
            status = _status_of(exc)
            retry_after = _retry_after_of(exc)
            if status in _VALIDATION_STATUS_CODES:
                logger.warning(
                    "Batch of %d rejected as invalid (status=%s) — retrying one-by-one to isolate the bad recipient(s): %s",
                    len(chunk), status, exc,
                )
                # Reuse the ABC's naive per-item loop as the fallback path.
                return super().send_batch(
                    chunk, from_email=from_email, from_name=from_name, reply_to=reply_to,
                    idempotency_key_prefix=f"{idempotency_key_prefix}:fallback" if idempotency_key_prefix else None,
                )
            # Auth/domain/quota/rate-limit/5xx/unknown: not item-specific.
            # Retry the whole chunk later, unchanged.
            error_msg = f"{type(exc).__name__}: {exc}"
            return [
                BatchItemResult(ref=job.ref, ok=False, error=error_msg, permanent=False,
                                 retry_after_seconds=retry_after)
                for job in chunk
            ]

        data = result.get("data", []) if isinstance(result, dict) else (getattr(result, "data", []) or [])
        errors = result.get("errors", []) if isinstance(result, dict) else (getattr(result, "errors", []) or [])

        errors_by_idx = {}
        for err in errors:
            idx = err.get("index") if isinstance(err, dict) else getattr(err, "index", None)
            msg = err.get("message") if isinstance(err, dict) else getattr(err, "message", None)
            if idx is not None:
                errors_by_idx[idx] = msg or "validation error"

        out: list[BatchItemResult] = []
        if len(data) == len(chunk):
            for i, job in enumerate(chunk):
                if i in errors_by_idx:
                    out.append(BatchItemResult(ref=job.ref, ok=False, error=errors_by_idx[i], permanent=True))
                else:
                    entry = data[i]
                    message_id = entry.get("id", "") if isinstance(entry, dict) else (getattr(entry, "id", "") or "")
                    out.append(BatchItemResult(ref=job.ref, ok=True, message_id=message_id))
        else:
            # data is compacted (only successful entries)
            data_iter = iter(data)
            for i, job in enumerate(chunk):
                if i in errors_by_idx:
                    out.append(BatchItemResult(ref=job.ref, ok=False, error=errors_by_idx[i], permanent=True))
                else:
                    try:
                        entry = next(data_iter)
                        message_id = entry.get("id", "") if isinstance(entry, dict) else (getattr(entry, "id", "") or "")
                        out.append(BatchItemResult(ref=job.ref, ok=True, message_id=message_id))
                    except StopIteration:
                        out.append(BatchItemResult(ref=job.ref, ok=False, error="missing from batch response", permanent=False))
        return out
