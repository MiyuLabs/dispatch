# MiyuLabs — Campaign Mailer

Lightweight, extensible, stateful campaign mailer and delivery engine for MiyuLabs. Built to reliably email waitlists and product announcements from a local machine or a $5 VPS without Redis or external job queues, which is more than enough for up to tens of thousands of sends. Imports recipients, renders campaign-specific templates, and delivers through Resend with SQLite-backed durable job state, concurrent rate-limited sending, automatic retries with exponential backoff, and idempotent campaign delivery.

<img width="1536" height="1024" alt="dispatch_architecture" src="https://github.com/user-attachments/assets/2ec1a762-b735-4202-8b27-9a909631701f" />

---

## Stack

| Layer | Tech |
|---|---|
| Language | Python 3.10+ |
| Queue & State Store | SQLite 3 (WAL mode, foreign keys enabled) |
| Email Provider | Resend API (`/emails/batch` with permissive validation) |
| Templating Engine | Jinja2 (HTML + responsive typography + plain text) |
| Deliverability Verification | `dnspython` (Syntax + MX + RFC 7505 Null MX / RFC 5321 fallback) |
| Rate Limiter | Thread-safe Token Bucket (`threading.Lock` + monotonic clock) |
| Environment & Packaging | `uv` / `pyproject.toml` (Setuptools) |

---

## Features

- **Embedded SQLite Job Queue:** SQLite handles both persistent recipient records and durable send queues. Zero external brokers (no Redis, Celery, or RabbitMQ) needed.
- **Idempotent by Design:** Enforced by a `UNIQUE(recipient_id, campaign)` schema constraint. Double-emailing someone for the same campaign is physically impossible.
- **Permissive Batch Delivery:** Bundles up to 100 emails per HTTP request with `x-batch-validation: permissive`. If 98 pass and 2 fail validation, Resend sends the 98 and Dispatch isolates the 2 failures in **one single API call** without looping one-by-one.
- **Bounce Protection via Enqueue Validation:** Verifies email syntax, rejects RFC 7505 Null MX records (`. IN MX 0 .`), and resolves domain MX records (with A-record fallback per RFC 5321) with in-process caching before queueing, keeping bounce rates safely below 4%.
- **Exponential Backoff with Full Jitter:** Resilient retry scheduling with jitter (`random(0, min(cap, base * 2^attempts))`) and dynamic adoption of provider `retry-after` header delay floors.
- **Automatic Crash & Interruption Recovery:** Stale jobs interrupted in `sending` state during unexpected shutdowns or restarts are automatically reclaimed and reset to `pending` on the next run.
- **Non-Destructive Dry Run:** Renders, substitutes metadata, logs, and validates emails end-to-end without mutating queue records or calling provider endpoints.

---

## Architecture


```
Source.fetch() -> Recipient(s) -> StateStore (SQLite: recipients)
                                          |
                              (Linked via campaign_recipients)
                                          |
                          RecipientValidator (syntax + MX) at enqueue time
                                          |
                       StateStore (SQLite: campaign_sends, the queue)
                                          |
                                SendEngine claims a batch
                                          |
                    Template.render(recipient) -> RenderedEmail, per job
                                          |
              EmailProvider.send_batch(...)  (Token Bucket, permissive mode)
                                          |
                  StateStore records sent / failed (+ backoff) / dead
```

### Core Abstractions

| Abstraction | Interface | Current Implementation |
|---|---|---|
| `Source` | `fetch() -> Iterable[Recipient]` | `JSONFileSource` (waitlist JSON array parser) |
| `RecipientValidator` | `validate(recipient) -> ValidationResult` | `SyntaxAndMXValidator` (graceful fallback to `SyntaxOnlyValidator`) |
| `Template` | `render(recipient) -> RenderedEmail` | `LaunchTemplate` (HTML + plain text pre-order discount) |
| `EmailProvider` | `send(...)`, `send_batch(...)` | `ResendProvider` (permissive batching + token bucket rate limit) |
| `StateStore` | Durable SQLite State & Queue | `StateStore` (WAL mode, atomic transactions, crash recovery) |

---

## Local Setup

### 1. Prerequisites
- Python 3.10+
- [`uv`](https://github.com/astral-sh/uv) (recommended) or `pip`

### 2. Install dependencies

```bash
uv sync
```

### 3. Environment variables

Copy the example and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description | Default |
|---|---|---|
| `RESEND_API_KEY` | Resend API key (`re_...`) | **Required** |
| `FROM_EMAIL` | Verified sender email address | **Required** |
| `FROM_NAME` | Sender display name | `MiyuLabs` |
| `REPLY_TO` | Reply-To address (optional) | *None* |
| `DISPATCH_DB_PATH` | Path to the SQLite state database file | `dispatch.db` |
| `PRE_ORDER_URL` | Destination URL for the primary CTA button | `https://miyulabs.in/` |
| `EXPLORE_URL` | Destination URL for secondary exploration link | `https://miyulabs.in/explore` |
| `RATE_LIMIT_PER_SECOND`| Max API calls/sec against Resend (batch calls count as 1) | `8` |
| `MAX_BATCH_SIZE` | Maximum recipients per batch request (Resend limit: 100) | `100` |
| `MAX_ATTEMPTS` | Maximum retry attempts for transient errors before job dies | `5` |
| `BASE_BACKOFF_SECONDS` | Initial backoff multiplier for retry jitter | `2` |
| `MAX_BACKOFF_SECONDS` | Maximum cap for exponential retry backoff | `600` |
| `VALIDATE_DELIVERABILITY`| Perform DNS MX check before queueing | `true` |
| `MX_LOOKUP_TIMEOUT_SECONDS`| DNS resolution timeout per domain | `3` |

Load environment variables into your active shell session:

```bash
set -a && source .env && set +a
```

---

## CLI Commands & Usage

All commands support `--db <path>` and `-v` / `--verbose` either before or after the subcommand.

### Command Reference

| Command | Arguments | Description |
|---|---|---|
| `dispatch import` | `--campaign <id>`<br>`--file <path>`<br>`[--source waitlist_json]` | Loads recipients from an export file into local state and links them to the specified campaign |
| `dispatch send` | `--campaign <id>`<br>`[--template <id>]`<br>`[--template-vars <json_or_file>]`<br>`[--dry-run]`<br>`[--watch]`<br>`[--max-wait <sec>]`<br>`[--skip-validation]` | Enqueues eligible recipients and executes email delivery for the given campaign |
| `dispatch status` | `--campaign <id>`<br>`[--limit <n>]` | Displays campaign stats, failures, and skipped records |

---

### Step-by-Step Walkthrough

#### 1. Import waitlist records
Loads and normalizes entries, deduplicating emails within the file and against existing database records, and linking them to a campaign:
```bash
uv run dispatch import --campaign launch-2026-09 --source waitlist_json --file data/waitlist.sample.json
```

#### 2. Test render & verify (Dry-Run)
Renders every email using the generic `--template-vars` config, logs subject lines and recipients, and runs deliverability validation without calling Resend or modifying database queue states:
```bash
uv run dispatch send --campaign launch-2026-09 --template launch --template-vars '{"solo_price": "5,999", "partner_price": "11,999"}' --dry-run
```

#### 3. Send campaign
Claims batches, executes delivery via Resend, and records results:
```bash
uv run dispatch send --campaign launch-2026-09 --template launch --template-vars '{"solo_price": "5,999", "partner_price": "11,999"}'
```

#### 4. Check campaign status
Inspect active progress, sent counts, retryable failures, and validation skips:
```bash
uv run dispatch status --campaign launch-2026-09
```

#### 5. Handling retries & large campaigns
If jobs enter backoff due to transient network or provider issues:
- **One-off / Interactive:** Pass `--watch` to keep the process running until all retry timers expire and the queue drains:
  ```bash
  uv run dispatch send --campaign launch-2026-09 --watch --max-wait 1800
  ```
- **Production / Scaled:** Run `dispatch send` on a systemd timer or cron job every few minutes. Each invocation claims eligible retries and exits immediately when pending work is done:
  ```bash
  */5 * * * * cd /opt/dispatch && set -a && source .env && set +a && uv run dispatch send --campaign launch-2026-09
  ```

---

## Extending Dispatch

### 1. Adding a new Source
Subclass `dispatch.sources.base.Source` and implement `fetch()` to yield `Recipient` models. Register it in `SOURCES` in `dispatch/cli.py`:
```python
from dispatch.sources.base import Source
from dispatch.models import Recipient

class CsvSource(Source):
    name = "csv"
    def fetch(self):
        # Read file, yield Recipient(email=..., metadata={...})
        ...
```
*Note: Any extra fields in source records are automatically preserved in `Recipient.metadata`.*

### 2. Customizing Templates & Personalization
`LaunchTemplate.render()` reads `recipient.metadata.get("name")` for personalized greetings. To branch copy (e.g. based on plan interest or user tier), subclass `dispatch.templates.base.Template` or modify template logic in `dispatch/templates/`:
```python
class CustomTemplate(Template):
    def render(self, recipient: Recipient) -> RenderedEmail:
        plan = recipient.metadata.get("planInterest", "solo")
        # Render custom HTML / subject based on plan
        ...
```

### 3. Adding an Email Provider
To use Amazon SES, Postmark, or SMTP, subclass `dispatch.providers.base.EmailProvider`. Implement `send()` (single send) and optionally override `send_batch()` if the provider offers a bulk endpoint:
```python
from dispatch.providers.base import EmailProvider, SendResult

class SesProvider(EmailProvider):
    def send(self, *, to, subject, html, text, from_email, **kwargs) -> SendResult:
        ...
```

### 4. Custom Validators
Subclass `dispatch.validation.RecipientValidator` to add third-party validation APIs or reject disposable email providers:
```python
from dispatch.validation import RecipientValidator, ValidationResult

class DisposableBlockerValidator(RecipientValidator):
    def validate(self, recipient: Recipient) -> ValidationResult:
        ...
```

---

## Pre-Flight Checklist & Notes

- **Resend Domain Verification:** Verify your sending domain in the Resend dashboard before dispatching. Emails sent from unverified domains will fail immediately.
- **DNS Deliverability Validation:** Deliverability checking queries DNS MX and A records while actively rejecting RFC 7505 Null MX records (domains that explicitly publish that they accept no mail). Results are cached in-memory per unique domain. For large waitlists, enqueueing may take a few moments on the first run while unique domains are resolved. Disable with `--skip-validation` or `VALIDATE_DELIVERABILITY=false` if needed.
- **Resend Rate Limits:** Resend enforces a default rate limit of 10 requests/second **shared across the entire account**. Because a batch of up to 100 emails counts as a single request, Dispatch can deliver up to 1,000 emails/second under this limit. Set `RATE_LIMIT_PER_SECOND` to 8 or lower if other services share the same Resend API key.
- **Unsubscribe & Compliance:** Dispatch is designed for opt-in waitlist announcements and does not include an automatic unsubscribe link handler out of the box. For recurring marketing broadcasts, include `List-Unsubscribe` headers or process opt-outs using `StateStore.mark_skipped()`.

---

## Testing

Run the automated test suite covering state management, batching, and validation:

```bash
uv run --with pytest pytest
```

---

## License

[MIT License](LICENSE).
Copyright (c) 2026 MiyuLabs.
