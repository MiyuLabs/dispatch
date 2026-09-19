from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .base import Source
from ..models import Recipient

logger = logging.getLogger(__name__)

# Fields we lift onto Recipient directly; anything else in a record is kept
# as personalization metadata automatically, so a richer future export
# (e.g. one that adds "name" or "planInterest") needs no code change here.
_KNOWN_FIELDS = {"email", "id", "createdAt", "source"}


class JSONFileSource(Source):
    """Reads recipients from a JSON array like the current waitlist export:

        [{"id": 1, "email": "a@b.com", "createdAt": "...", "source": "waitlist"}, ...]
    """

    name = "waitlist_json"

    def __init__(self, path: str | Path, default_source_label: str = "waitlist"):
        self.path = Path(path)
        self.default_source_label = default_source_label

    def fetch(self) -> Iterator[Recipient]:
        if not self.path.exists():
            raise FileNotFoundError(f"Source file not found: {self.path}")

        with self.path.open("r", encoding="utf-8") as f:
            raw = json.load(f)

        if not isinstance(raw, list):
            raise ValueError(f"{self.path} must contain a JSON array of entries")

        seen_in_file: set[str] = set()
        for entry in raw:
            try:
                recipient = self._parse(entry)
            except (KeyError, TypeError, ValueError) as e:
                logger.warning("Skipping malformed entry %r: %s", entry, e)
                continue
            if recipient.email in seen_in_file:
                logger.info("Duplicate email within source file, skipping: %s", recipient.email)
                continue
            seen_in_file.add(recipient.email)
            yield recipient

    def _parse(self, entry: dict) -> Recipient:
        if "email" not in entry or not entry["email"]:
            raise KeyError("email")
        email = str(entry["email"]).strip().lower()
        if "@" not in email or " " in email:
            raise ValueError(f"invalid email: {email!r}")

        created_at = None
        raw_created = entry.get("createdAt")
        if raw_created:
            try:
                created_at = datetime.fromisoformat(str(raw_created).replace("Z", "+00:00"))
            except ValueError:
                logger.debug("Unparseable createdAt %r for %s", raw_created, email)

        return Recipient(
            email=email,
            source=str(entry.get("source") or self.default_source_label),
            external_id=str(entry["id"]) if entry.get("id") is not None else None,
            created_at=created_at,
            metadata={k: v for k, v in entry.items() if k not in _KNOWN_FIELDS},
        )
