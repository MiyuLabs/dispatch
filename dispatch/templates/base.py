from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import Recipient, RenderedEmail


class Template(ABC):
    """Turns one Recipient into a rendered email.

    Today every recipient gets the same copy. When per-segment or
    per-recipient personalization is needed, it happens here — read
    whatever fields you need off `recipient.metadata` (populated by the
    Source) and branch or substitute accordingly. The orchestrator
    (engine.py) never needs to know a template got smarter.
    """

    name: str

    @abstractmethod
    def render(self, recipient: Recipient) -> RenderedEmail:
        raise NotImplementedError
