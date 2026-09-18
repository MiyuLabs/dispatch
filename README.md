# dispatch

Lightweight, resumable campaign mailer for MiyuLabs. Imports recipients from external sources, renders campaign-specific emails, and delivers them through Resend with SQLite-backed durable job state, concurrent rate-limited sending, automatic retries with exponential backoff, and idempotent campaign delivery.
