"""
Sliding Window Counter Algorithm
---------------------------------
Tracks the exact timestamps of recent requests in a Redis Sorted Set.
- Window = rolling last N seconds from NOW (not a fixed clock boundary).
- On each request: prune old entries, count remaining, allow if under limit.
- Stricter and fairer than fixed window — no burst at boundary edge.
- Higher memory use than token bucket (stores one entry per request).

Redis key:
  rl:sw:{client_id}:{endpoint} → Sorted Set, score = request timestamp
"""

import time
import redis.asyncio as aioredis
from dataclasses import dataclass


@dataclass
class SlidingWindowResult:
    allowed: bool
    requests_in_window: int
    limit: int
    window_seconds: int
    retry_after: float | None   # seconds until oldest request falls out of window


class SlidingWindow:
    def __init__(
        self,
        limit: int = 100,           # max requests in window
        window_seconds: int = 60,   # rolling window size
    ):
        self.limit = limit
        self.window_seconds = window_seconds

    def _key(self, client_id: str, endpoint: str) -> str:
        return f"rl:sw:{client_id}:{endpoint}"

    async def check(
        self,
        redis: aioredis.Redis,
        client_id: str,
        endpoint: str,
    ) -> SlidingWindowResult:
        key = self._key(client_id, endpoint)
        now = time.time()
        window_start = now - self.window_seconds

        # Atomic Lua script:
        # 1. Remove entries older than the window
        # 2. Count remaining entries
        # 3. If under limit, add this request
        # 4. Set TTL so key auto-expires
        lua_script = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window_start = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])
        local window_seconds = tonumber(ARGV[4])

        -- Remove timestamps outside the current window
        redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

        -- Count requests in current window
        local count = redis.call('ZCARD', key)

        local allowed = 0
        if count < limit then
            -- Add current request timestamp as both score and member
            -- Using now + random suffix to handle sub-millisecond duplicate scores
            redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
            count = count + 1
            allowed = 1
        end

        -- Auto-expire key after window passes
        redis.call('EXPIRE', key, window_seconds + 1)

        -- Return: allowed, current count, oldest entry score
        local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
        local oldest_score = oldest[2] or now

        return {allowed, count, oldest_score}
        """

        result = await redis.eval(
            lua_script, 1, key,
            now, window_start, self.limit, self.window_seconds
        )

        allowed = bool(result[0])
        count = int(result[1])
        oldest_score = float(result[2])

        retry_after = None
        if not allowed:
            # Time until the oldest request falls outside the window
            retry_after = round((oldest_score + self.window_seconds) - now, 3)
            retry_after = max(0.0, retry_after)

        return SlidingWindowResult(
            allowed=allowed,
            requests_in_window=count,
            limit=self.limit,
            window_seconds=self.window_seconds,
            retry_after=retry_after,
        )
