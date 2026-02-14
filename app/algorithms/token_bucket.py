"""
Token Bucket Algorithm
----------------------
Each client gets a bucket with a fixed capacity of tokens.
- Every request consumes 1 token.
- Tokens refill at a steady rate (e.g. 10/sec).
- If the bucket is empty → request is DENIED.
- Allows short bursts (client can use all tokens at once if bucket is full).

Redis keys:
  rl:tb:{client_id}:{endpoint}:tokens   → current token count (float)
  rl:tb:{client_id}:{endpoint}:last_refill → timestamp of last refill
"""

import time
import redis.asyncio as aioredis
from dataclasses import dataclass


@dataclass
class TokenBucketResult:
    allowed: bool
    tokens_remaining: float
    capacity: int
    refill_rate: float          # tokens per second
    retry_after: float | None   # seconds until next token available


class TokenBucket:
    def __init__(
        self,
        capacity: int = 100,        # max tokens in bucket
        refill_rate: float = 10.0,  # tokens added per second
    ):
        self.capacity = capacity
        self.refill_rate = refill_rate

    def _keys(self, client_id: str, endpoint: str) -> tuple[str, str]:
        base = f"rl:tb:{client_id}:{endpoint}"
        return f"{base}:tokens", f"{base}:last_refill"

    async def check(
        self,
        redis: aioredis.Redis,
        client_id: str,
        endpoint: str,
    ) -> TokenBucketResult:
        token_key, refill_key = self._keys(client_id, endpoint)
        now = time.time()

        # Lua script for atomic read-compute-write
        # Lua runs atomically in Redis — no race conditions across instances
        lua_script = """
        local token_key = KEYS[1]
        local refill_key = KEYS[2]
        local now = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local refill_rate = tonumber(ARGV[3])

        -- Read current state (default: full bucket, current time)
        local tokens = tonumber(redis.call('GET', token_key)) or capacity
        local last_refill = tonumber(redis.call('GET', refill_key)) or now

        -- Calculate tokens to add since last refill
        local elapsed = now - last_refill
        local new_tokens = math.min(capacity, tokens + (elapsed * refill_rate))

        local allowed = 0
        if new_tokens >= 1 then
            new_tokens = new_tokens - 1
            allowed = 1
        end

        -- Persist updated state with TTL (expire after bucket would fully refill)
        local ttl = math.ceil(capacity / refill_rate) + 10
        redis.call('SET', token_key, new_tokens, 'EX', ttl)
        redis.call('SET', refill_key, now, 'EX', ttl)

        return {allowed, new_tokens}
        """

        result = await redis.eval(lua_script, 2, token_key, refill_key, now, self.capacity, self.refill_rate)
        allowed = bool(result[0])
        tokens_remaining = float(result[1])

        retry_after = None
        if not allowed:
            # Time until 1 token is available
            retry_after = round((1 - tokens_remaining) / self.refill_rate, 3)

        return TokenBucketResult(
            allowed=allowed,
            tokens_remaining=round(tokens_remaining, 2),
            capacity=self.capacity,
            refill_rate=self.refill_rate,
            retry_after=retry_after,
        )
