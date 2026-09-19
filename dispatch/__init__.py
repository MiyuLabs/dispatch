"""dispatch — a small, extensible waitlist/campaign mailer for MiyuLabs.

Architecture in one paragraph: `Source`s produce `Recipient`s, which are
upserted into a local SQLite `StateStore`. Sending a campaign enqueues one
durable job per (recipient, campaign) pair in that same store, a worker
pool claims jobs in batches, renders them with a `Template`, and sends them
through an `EmailProvider` (Resend, today). Every layer is an abstract base
class with one concrete implementation — swap or add implementations
without touching the orchestration in `engine.py`.
"""

__version__ = "0.1.0"
