from __future__ import annotations

import threading
import time


class TokenBucket:
    """Thread-safe token bucket, used to keep us under the provider's
    requests-per-second limit regardless of how many worker threads we run.

    Resend's default is a handful of requests/sec per team (check your
    dashboard — it varies by plan and can be raised on request). Keep
    RATE_LIMIT_PER_SECOND comfortably under whatever your account allows.
    """

    def __init__(self, rate_per_second: float, burst: int | None = None):
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        self.rate = rate_per_second
        self.capacity = burst if burst is not None else max(1, int(rate_per_second))
        self._tokens = float(self.capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def acquire(self) -> None:
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                deficit = 1 - self._tokens
                wait_time = deficit / self.rate
            time.sleep(wait_time)
