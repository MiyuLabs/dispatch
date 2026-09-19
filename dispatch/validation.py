from __future__ import annotations

import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from .models import Recipient

logger = logging.getLogger(__name__)

try:
    import dns.resolver
    _DNS_AVAILABLE = True
except ImportError:  # dnspython not installed
    _DNS_AVAILABLE = False

DNS_AVAILABLE = _DNS_AVAILABLE  # public alias for callers outside this module

# Deliberately permissive — this is a first line of defense against typos
# and junk, not a full RFC 5322 parser. We'd rather let a slightly unusual
# but real address through than reject something valid.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    reason: Optional[str] = None  # only set when valid=False


class RecipientValidator(ABC):
    """Anything that can decide whether a recipient is worth attempting to
    email. Runs once per recipient at enqueue time, before a job ever
    enters the send queue — an invalid recipient is recorded as `skipped`
    rather than burning a send attempt (and a bounce) on it.

    This exists because Resend (like most providers) asks senders to keep
    bounce rate under ~4%; catching "no mail server for this domain" and
    "not even shaped like an email address" locally costs nothing and
    prevents the most common source of bounces from a self-reported
    waitlist (typos). It cannot catch "mailbox doesn't exist" — that only
    a real delivery attempt (or an unreliable SMTP handshake most
    providers block anyway) can tell you.
    """

    @abstractmethod
    def validate(self, recipient: Recipient) -> ValidationResult:
        raise NotImplementedError


class SyntaxOnlyValidator(RecipientValidator):
    """No network calls — just shape-of-an-email-address checking. Used as
    the automatic fallback when dnspython isn't installed or MX checks are
    disabled, so validation degrades gracefully instead of failing shut."""

    def validate(self, recipient: Recipient) -> ValidationResult:
        if not _EMAIL_RE.match(recipient.email):
            return ValidationResult(valid=False, reason="malformed address")
        return ValidationResult(valid=True)


class SyntaxAndMXValidator(RecipientValidator):
    """Syntax check, then an MX record lookup for the domain (falling back
    to an A/AAAA record per RFC 5321 §5 — some domains accept mail with no
    MX record at all). Domain results are cached in-process for `ttl_seconds`
    since waitlists cluster heavily on a handful of domains (gmail.com,
    outlook.com, ...) — a 1000-row import might only be a few dozen unique
    domains worth of actual DNS traffic.
    """

    def __init__(self, timeout_seconds: float = 3.0, ttl_seconds: float = 3600.0):
        if not _DNS_AVAILABLE:
            raise RuntimeError("dnspython is not installed — use SyntaxOnlyValidator, or `pip install dnspython`")
        self._timeout = timeout_seconds
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[float, ValidationResult]] = {}

    def validate(self, recipient: Recipient) -> ValidationResult:
        if not _EMAIL_RE.match(recipient.email):
            return ValidationResult(valid=False, reason="malformed address")

        domain = recipient.email.rsplit("@", 1)[-1].lower()
        cached = self._cache.get(domain)
        if cached and (time.monotonic() - cached[0]) < self._ttl:
            return cached[1]

        result = self._check_domain(domain)
        self._cache[domain] = (time.monotonic(), result)
        return result

    def _check_domain(self, domain: str) -> ValidationResult:
        resolver = dns.resolver.Resolver()
        resolver.lifetime = self._timeout
        try:
            answers = resolver.resolve(domain, "MX")
            if len(answers) > 0:
                # RFC 7505: "Null MX" (single record with preference 0 and exchange ".").
                # Explicitly declares that the domain does not accept email.
                # Sending MTAs MUST NOT attempt delivery and MUST NOT fall back to A/AAAA records.
                first = answers[0]
                is_null_mx = (
                    len(answers) == 1
                    and getattr(first, "preference", None) == 0
                    and str(getattr(first, "exchange", "")).rstrip(".") == ""
                )
                if is_null_mx:
                    return ValidationResult(
                        valid=False,
                        reason=f"domain does not accept email (RFC 7505 Null MX): {domain}",
                    )
                return ValidationResult(valid=True)
        except dns.resolver.NXDOMAIN:
            return ValidationResult(valid=False, reason=f"domain does not exist: {domain}")
        except dns.resolver.NoAnswer:
            pass  # no MX record — fall through to the A-record fallback below
        except Exception as exc:  # timeout, SERVFAIL, etc. — inconclusive, not the recipient's fault
            logger.warning("MX lookup failed for %s (%s) — treating as valid rather than guessing", domain, exc)
            return ValidationResult(valid=True)

        try:
            resolver.resolve(domain, "A")
            return ValidationResult(valid=True)  # RFC 5321: A record is a valid MX fallback
        except dns.resolver.NXDOMAIN:
            return ValidationResult(valid=False, reason=f"no MX or A record for domain: {domain}")
        except Exception as exc:
            logger.warning("A-record fallback lookup failed for %s (%s) — treating as valid", domain, exc)
            return ValidationResult(valid=True)
