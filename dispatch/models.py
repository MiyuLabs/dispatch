from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class Recipient:
    """A single addressable person, independent of where they came from."""

    email: str
    source: str = "unknown"
    external_id: Optional[str] = None
    created_at: Optional[datetime] = None
    # Free-form bag for personalization fields (name, plan interest, etc).
    # Sources decide what goes in here; Templates decide what they read.
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RenderedEmail:
    subject: str
    html: str
    text: str
