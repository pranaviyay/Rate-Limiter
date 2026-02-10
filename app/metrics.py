"""
Metrics
-------
In-memory request counters. Lightweight — no external dependencies.
Resets on service restart (acceptable for a middleware service).

Tracks:
- Total requests checked
- Allowed vs denied counts
- Redis failure count (fail-open events)
- Per-endpoint breakdown
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict
import time


@dataclass
class Metrics:
    total_requests: int = 0
    allowed: int = 0
    denied: int = 0
    redis_failures: int = 0          # times we hit the Redis fallback
    started_at: float = field(default_factory=time.time)

    # Per-endpoint counters: endpoint -> {allowed, denied}
    by_endpoint: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: {"allowed": 0, "denied": 0})
    )

    def record(self, endpoint: str, was_allowed: bool):
        self.total_requests += 1
        if was_allowed:
            self.allowed += 1
            self.by_endpoint[endpoint]["allowed"] += 1
        else:
            self.denied += 1
            self.by_endpoint[endpoint]["denied"] += 1

    def record_redis_failure(self):
        self.redis_failures += 1

    def summary(self) -> dict:
        uptime_seconds = int(time.time() - self.started_at)
        deny_rate = (
            round(self.denied / self.total_requests * 100, 2)
            if self.total_requests > 0 else 0.0
        )
        return {
            "uptime_seconds": uptime_seconds,
            "total_requests": self.total_requests,
            "allowed": self.allowed,
            "denied": self.denied,
            "deny_rate_percent": deny_rate,
            "redis_failures": self.redis_failures,
            "by_endpoint": dict(self.by_endpoint),
        }


# Single global instance shared across requests
metrics = Metrics()
