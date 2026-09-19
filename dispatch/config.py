from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    resend_api_key: str
    from_email: str
    from_name: str
    reply_to: Optional[str]

    db_path: str

    # Requests/sec against Resend's API — shared across the whole team, not
    # just this process. Default 10, but a batch call (up to 100 recipients)
    # only costs ONE request, so this limits *API calls*, not emails/sec.
    # If other tools or teammates also call the API, set this lower than 10
    # to leave them headroom.
    rate_limit_per_second: float
    max_batch_size: int

    max_attempts: int
    base_backoff_seconds: float
    max_backoff_seconds: float

    validate_deliverability: bool
    mx_lookup_timeout_seconds: float

    pre_order_url: str
    explore_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        def get(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
            val = os.environ.get(name, default)
            if required and not val:
                raise ConfigError(f"Missing required environment variable: {name}")
            return val

        def get_float(name: str, default: float) -> float:
            raw = os.environ.get(name)
            return float(raw) if raw else default

        def get_int(name: str, default: int) -> int:
            raw = os.environ.get(name)
            return int(raw) if raw else default

        def get_bool(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            if raw is None:
                return default
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        pre_order_url = get("PRE_ORDER_URL", "https://miyulabs.in/")
        return cls(
            resend_api_key=get("RESEND_API_KEY", required=True),
            from_email=get("FROM_EMAIL", required=True),
            from_name=get("FROM_NAME", "MiyuLabs"),
            reply_to=get("REPLY_TO", None),
            db_path=get("DISPATCH_DB_PATH", "dispatch.db"),
            rate_limit_per_second=get_float("RATE_LIMIT_PER_SECOND", 8.0),
            max_batch_size=get_int("MAX_BATCH_SIZE", 100),
            max_attempts=get_int("MAX_ATTEMPTS", 5),
            base_backoff_seconds=get_float("BASE_BACKOFF_SECONDS", 2.0),
            max_backoff_seconds=get_float("MAX_BACKOFF_SECONDS", 600.0),
            validate_deliverability=get_bool("VALIDATE_DELIVERABILITY", True),
            mx_lookup_timeout_seconds=get_float("MX_LOOKUP_TIMEOUT_SECONDS", 3.0),
            pre_order_url=pre_order_url,
            explore_url=get("EXPLORE_URL", f"{pre_order_url.rstrip('/')}/explore"),
        )
