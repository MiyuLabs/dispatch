from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Iterator

from ..models import Recipient


class Source(ABC):
    """Anything that can produce a list of Recipients.

    To add a new source (a second waitlist tool, a Google Sheet export, a
    CRM export, direct signups from the live site, ...), subclass this and
    implement `fetch()`. Nothing else in the codebase needs to change —
    `dispatch import --source <name>` just needs a registry entry in
    cli.py pointing at the new class.

    Contract: `fetch()` should skip and log malformed individual records
    rather than raising, so one bad row in a 1000-row export doesn't abort
    the whole import.
    """

    name: str

    @abstractmethod
    def fetch(self) -> Iterable[Recipient] | Iterator[Recipient]:
        raise NotImplementedError
