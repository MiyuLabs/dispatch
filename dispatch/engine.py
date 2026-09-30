from __future__ import annotations

import hashlib
import json
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from .config import Settings
from .db import StateStore
from .models import Recipient
from .providers.base import BatchItemResult, EmailJob, EmailProvider
from .sources.base import Source
from .templates.base import Template
from .validation import RecipientValidator

logger = logging.getLogger(__name__)


@dataclass
class ImportStats:
    read: int = 0
    new: int = 0
    already_known: int = 0


@dataclass
class EnqueueStats:
    queued: int = 0
    skipped_invalid: int = 0
    already_queued: int = 0


@dataclass
class SendStats:
    claimed: int = 0
    sent: int = 0
    failed_retryable: int = 0
    failed_exhausted: int = 0


def import_recipients(source: Source, store: StateStore, campaign: str) -> ImportStats:
    """Pull recipients from a Source and upsert them into the state store,
    linking them to the given campaign.
    Never sends anything — importing and enqueueing a campaign are
    separate, deliberate steps."""
    stats = ImportStats()
    for recipient in source.fetch():
        stats.read += 1
        _, was_new = store.upsert_recipient(recipient, campaign=campaign)
        if was_new:
            stats.new += 1
        else:
            stats.already_known += 1
    return stats


def enqueue_campaign(store: StateStore, campaign: str, validator: Optional[RecipientValidator] = None) -> EnqueueStats:
    """Create a send job for every known recipient that doesn't already
    have one for this campaign. Idempotent — safe to re-run.

    If a validator is given, each new recipient is checked before it
    becomes a job: one that fails validation (bad syntax, no MX record)
    is recorded straight away with status='skipped' — visible in
    `dispatch status`, but never queued for an actual send attempt or
    provider API call. This is the mechanism that keeps typo'd and
    dead-domain addresses from ever contributing to bounce rate.
    """
    stats = EnqueueStats()
    total_recipients = store.count_recipients(campaign)
    unenqueued = store.unenqueued_recipients(campaign)
    stats.already_queued = total_recipients - len(unenqueued)

    for row in unenqueued:
        recipient = _row_to_recipient(row)
        rid = row["id"]

        if validator is not None:
            result = validator.validate(recipient)
            if not result.valid:
                if store.enqueue_skipped(rid, campaign, result.reason or "failed validation"):
                    stats.skipped_invalid += 1
                else:
                    stats.already_queued += 1
                continue

        if store.enqueue(rid, campaign):
            stats.queued += 1
        else:
            stats.already_queued += 1
    return stats


def _row_to_recipient(row) -> Recipient:
    metadata = {}
    if row["metadata"]:
        try:
            metadata = json.loads(row["metadata"])
        except json.JSONDecodeError:
            metadata = {}
    created_at = None
    if row["source_created_at"]:
        try:
            created_at = datetime.fromisoformat(row["source_created_at"])
        except ValueError:
            pass
    return Recipient(
        email=row["email"],
        source=row["source"],
        external_id=row["external_id"],
        created_at=created_at,
        metadata=metadata,
    )


def _next_attempt_at(attempts: int, base: float, cap: float, floor_seconds: Optional[float] = None) -> str:
    """Exponential backoff with full jitter: base * 2^attempts, capped,
    then a random draw in [0, that]. `floor_seconds` (from a provider's
    `retry-after` hint) sets a minimum wait so we don't ignore a rate
    limiter that told us exactly how long to back off."""
    ceiling = min(cap, base * (2 ** attempts))
    delay = random.uniform(0, ceiling)
    if floor_seconds:
        delay = max(delay, floor_seconds)
    return (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()


def _compute_submission_idempotency_key(campaign: str, send_ids: list[int]) -> str:
    digest = hashlib.sha256(",".join(sorted(str(sid) for sid in send_ids)).encode()).hexdigest()[:24]
    return f"dispatch:{campaign}:batch:{digest}"


def _call_provider_send_batch(
    provider: EmailProvider,
    jobs: list[EmailJob],
    *,
    from_email: str,
    from_name: Optional[str],
    reply_to: Optional[str],
    idempotency_key_prefix: Optional[str],
    idempotency_key: Optional[str],
) -> list[BatchItemResult]:
    try:
        return provider.send_batch(
            jobs,
            from_email=from_email,
            from_name=from_name,
            reply_to=reply_to,
            idempotency_key_prefix=idempotency_key_prefix,
            idempotency_key=idempotency_key,
        )
    except TypeError as e:
        if "idempotency_key" in str(e):
            return provider.send_batch(
                jobs,
                from_email=from_email,
                from_name=from_name,
                reply_to=reply_to,
                idempotency_key_prefix=idempotency_key_prefix,
            )
        raise


class SendEngine:
    """Drains the campaign_sends queue for one campaign: claim a batch,
    render each job, hand the whole batch to the provider in one call
    (`EmailProvider.send_batch`), record outcomes, repeat.

    There's deliberately no thread pool here. Batching collapses what
    used to be one HTTP call per recipient into one call per ~100
    recipients, and Resend's rate limit (a handful of requests/sec, shared
    across the team) is the real ceiling either way — concurrency can't
    push total throughput past what the rate limiter allows, so a single
    thread issuing batch calls is exactly as fast as several threads would
    be, with far less code.
    """

    def __init__(self, store: StateStore, provider: EmailProvider, template: Template, settings: Settings, dry_run: bool = False):
        self.store = store
        self.provider = provider
        self.template = template
        self.settings = settings
        self.dry_run = dry_run

    def run_once(self, campaign: str, batch_size: int = 500) -> SendStats:
        """Claim and process everything currently eligible. Does not wait
        for future-scheduled retries — call again later (cron, a loop, or
        `run_until_drained`) to pick those up. `batch_size` is how many
        rows we claim from SQLite per round; the provider further chunks
        that into its own API batch limit (100 for Resend) internally."""
        stale_threshold = self.settings.sending_stale_after_seconds

        stats = SendStats()
        if not self.dry_run:
            self.recover_stale_submissions(campaign, stats=stats, older_than_seconds=stale_threshold)
            recovered = self.store.recover_stale_sending_jobs(campaign, older_than_seconds=stale_threshold)
            if recovered:
                logger.info("Recovered %d stale 'sending' job(s) from previous run back to 'pending'", recovered)

        if self.dry_run:
            rows = self.store.pending_jobs_for_preview(campaign, limit=batch_size)
            stats.claimed += len(rows)
            for row in rows:
                recipient = _row_to_recipient(row)
                try:
                    rendered = self.template.render(recipient)
                    logger.info("[dry-run] would send %r to %s", rendered.subject, recipient.email)
                    stats.sent += 1
                except Exception as exc:
                    logger.exception("[dry-run] Template render failed for %s", recipient.email)
                    stats.failed_exhausted += 1
            return stats

        while True:
            batch = self.store.claim_batch(campaign, batch_size)
            if not batch:
                break
            stats.claimed += len(batch)
            self._process_batch(campaign, batch, stats)
        return stats

    def recover_stale_submissions(
        self,
        campaign: str,
        stats: Optional[SendStats] = None,
        older_than_seconds: Optional[float] = None,
    ) -> int:
        """Finds in-flight / ambiguous provider submissions that have passed the
        stale threshold and retries them using the EXACT SAME batch membership,
        ordering, and provider idempotency key."""
        if older_than_seconds is None:
            older_than_seconds = self.settings.sending_stale_after_seconds

        stale_subs = self.store.get_stale_submissions(campaign, older_than_seconds=older_than_seconds)
        if not stale_subs:
            return 0

        recovered_count = 0
        for sub in stale_subs:
            sub_id = sub["id"]
            idempotency_key = sub["idempotency_key"]
            try:
                send_ids = json.loads(sub["send_ids"])
            except Exception:
                continue

            rows = self.store.get_jobs_by_ids(campaign, send_ids)
            if not rows:
                self.store.complete_submission(sub_id)
                continue

            unresolved_rows = [r for r in rows if r["status"] in ("submitted", "sending")]
            if not unresolved_rows:
                self.store.complete_submission(sub_id)
                continue

            logger.info(
                "Recovering ambiguous provider submission %s (idempotency_key=%s, %d job(s))",
                sub_id, idempotency_key, len(send_ids),
            )

            self._execute_chunk(
                campaign=campaign,
                send_ids=send_ids,
                rows=rows,
                submission_id=sub_id,
                idempotency_key=idempotency_key,
                stats=stats,
                is_recovery=True,
            )
            recovered_count += 1

        return recovered_count

    def run_until_drained(self, campaign: str, batch_size: int = 500, max_wait_seconds: float = 3600) -> SendStats:
        """Convenience for small/medium lists: process everything eligible
        now, and if some jobs are only waiting on backoff, sleep until
        retrying makes sense — until the queue is fully drained or
        max_wait_seconds elapses. For very large campaigns, prefer
        `run_once` on a cron/systemd timer instead of holding a process
        open."""
        import time as _time

        total = SendStats()
        started = _time.monotonic()
        while True:
            stats = self.run_once(campaign, batch_size)
            total.claimed += stats.claimed
            total.sent += stats.sent
            total.failed_retryable += stats.failed_retryable
            total.failed_exhausted += stats.failed_exhausted

            if self.dry_run:
                break

            still_pending_retry = self.store.campaign_stats(campaign).get("failed", 0)
            if still_pending_retry == 0:
                break
            if _time.monotonic() - started > max_wait_seconds:
                logger.warning("max_wait_seconds reached with %d jobs still in backoff", still_pending_retry)
                break
            _time.sleep(min(30, self.settings.base_backoff_seconds))
        return total

    def _process_batch(self, campaign: str, rows: list, stats: SendStats) -> None:
        chunk_size = self.settings.max_batch_size
        for start in range(0, len(rows), chunk_size):
            chunk_rows = rows[start:start + chunk_size]
            send_ids = [r["send_id"] for r in chunk_rows]
            idempotency_key = _compute_submission_idempotency_key(campaign, send_ids)
            submission_id = f"sub_{campaign}_{hashlib.sha256(idempotency_key.encode()).hexdigest()[:16]}"
            self._execute_chunk(
                campaign=campaign,
                send_ids=send_ids,
                rows=chunk_rows,
                submission_id=submission_id,
                idempotency_key=idempotency_key,
                stats=stats,
                is_recovery=False,
            )

    def _execute_chunk(
        self,
        campaign: str,
        send_ids: list[int],
        rows: list,
        submission_id: str,
        idempotency_key: str,
        stats: Optional[SendStats] = None,
        is_recovery: bool = False,
    ) -> None:
        rows_by_send_id = {row["send_id"]: row for row in rows}
        jobs: list[EmailJob] = []

        for row in rows:
            recipient = _row_to_recipient(row)
            try:
                rendered = self.template.render(recipient)
            except Exception as exc:
                logger.exception("Template render failed for %s", recipient.email)
                self.store.mark_failed(row["send_id"], row["attempts"] + 1, f"render error: {exc}", next_attempt_at=None)
                if stats:
                    stats.failed_exhausted += 1
                continue
            jobs.append(EmailJob(ref=row["send_id"], to=recipient.email, subject=rendered.subject, html=rendered.html, text=rendered.text))

        if not jobs:
            self.store.complete_submission(submission_id)
            return

        if not is_recovery:
            self.store.record_submission(submission_id, campaign, idempotency_key, send_ids)

        if is_recovery and stats is not None:
            stats.claimed += len(jobs)

        try:
            results = _call_provider_send_batch(
                self.provider,
                jobs,
                from_email=self.settings.from_email,
                from_name=self.settings.from_name,
                reply_to=self.settings.reply_to,
                idempotency_key_prefix=f"dispatch:{campaign}",
                idempotency_key=idempotency_key,
            )
        except Exception as exc:
            logger.warning(
                "Ambiguous provider outcome for submission %s: %s", submission_id, exc
            )
            self.store.mark_submission_ambiguous(submission_id, str(exc))
            return

        has_ambiguous = any(getattr(r, "ambiguous", False) for r in results)
        if has_ambiguous:
            err_msg = next((r.error for r in results if getattr(r, "ambiguous", False) and r.error), "ambiguous provider outcome")
            logger.warning("Provider returned ambiguous result for submission %s: %s", submission_id, err_msg)
            self.store.mark_submission_ambiguous(submission_id, err_msg)
            return

        for result in results:
            row = rows_by_send_id[result.ref]
            attempts = row["attempts"] + 1
            if result.ok:
                self.store.mark_sent(result.ref, result.message_id)
                if stats:
                    stats.sent += 1
            elif result.permanent:
                self.store.mark_failed(result.ref, attempts, result.error or "permanent failure", next_attempt_at=None)
                if stats:
                    stats.failed_exhausted += 1
            elif attempts >= self.settings.max_attempts:
                self.store.mark_failed(result.ref, attempts, result.error or "retries exhausted", next_attempt_at=None)
                if stats:
                    stats.failed_exhausted += 1
            else:
                next_at = _next_attempt_at(
                    attempts, self.settings.base_backoff_seconds, self.settings.max_backoff_seconds,
                    floor_seconds=result.retry_after_seconds,
                )
                self.store.mark_failed(result.ref, attempts, result.error or "transient failure", next_attempt_at=next_at)
                if stats:
                    stats.failed_retryable += 1

        self.store.complete_submission(submission_id)
