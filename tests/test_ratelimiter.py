"""
Unit Tests — Distributed Rate Limiter Service
----------------------------------------------
Tests core algorithm logic, config store, and API endpoints.

Run with:
    docker exec -it ratelimiter-app-1 pytest tests/ -v
"""

import pytest
import time
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch, MagicMock

from app.main import app
from app.algorithms.sliding_window import SlidingWindow
from app.algorithms.token_bucket import TokenBucket
from app.config_store import set_config, get_config, delete_config


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_redis():
    """
    In-memory fake Redis using a dict — no real Redis needed for unit tests.
    Supports: get, set, delete, eval, zremrangebyscore, zcard, zrange, expire, ping
    """
    store = {}
    scores = {}  # for sorted sets: key -> list of (score, member)

    redis = AsyncMock()

    async def fake_get(key):
        return store.get(key)

    async def fake_set(key, value, ex=None):
        store[key] = str(value)
        return True

    async def fake_delete(*keys):
        count = sum(1 for k in keys if k in store)
        for k in keys:
            store.pop(k, None)
        return count

    async def fake_ping():
        return True

    async def fake_zcard(key):
        return len(scores.get(key, []))

    async def fake_zremrangebyscore(key, min_score, max_score):
        if key in scores:
            scores[key] = [(s, m) for s, m in scores[key] if s > float(max_score)]

    async def fake_zrange(key, start, end, withscores=False):
        items = sorted(scores.get(key, []), key=lambda x: x[0])
        sliced = items[start: end + 1 if end != -1 else None]
        if withscores:
            return [(m, s) for s, m in sliced]
        return [m for s, m in sliced]

    async def fake_expire(key, ttl):
        return True

    # Lua script simulation for sliding window
    async def fake_eval(script, num_keys, *args):
        key = args[0]
        now = float(args[1])
        window_start = float(args[2])
        limit = int(args[3])
        window_seconds = int(args[4])

        if key not in scores:
            scores[key] = []

        # Prune old entries
        scores[key] = [(s, m) for s, m in scores[key] if s > window_start]

        count = len(scores[key])
        allowed = 0

        if count < limit:
            member = f"{now}-{len(scores[key])}"
            scores[key].append((now, member))
            count += 1
            allowed = 1

        oldest = scores[key][0][0] if scores[key] else now
        return [allowed, count, oldest]

    redis.get = fake_get
    redis.set = fake_set
    redis.delete = fake_delete
    redis.ping = fake_ping
    redis.zcard = fake_zcard
    redis.zremrangebyscore = fake_zremrangebyscore
    redis.zrange = fake_zrange
    redis.expire = fake_expire
    redis.eval = fake_eval

    return redis


@pytest_asyncio.fixture
async def client():
    """Async test client for FastAPI app."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


# ── Sliding Window Tests ───────────────────────────────────────────────────────

class TestSlidingWindow:

    @pytest.mark.asyncio
    async def test_allows_requests_under_limit(self, mock_redis):
        limiter = SlidingWindow(limit=5, window_seconds=60)
        for i in range(5):
            result = await limiter.check(mock_redis, "user_1", "/api/test")
            assert result.allowed is True, f"Request {i+1} should be allowed"

    @pytest.mark.asyncio
    async def test_denies_at_limit(self, mock_redis):
        limiter = SlidingWindow(limit=5, window_seconds=60)
        for _ in range(5):
            await limiter.check(mock_redis, "user_1", "/api/test")
        result = await limiter.check(mock_redis, "user_1", "/api/test")
        assert result.allowed is False

    @pytest.mark.asyncio
    async def test_different_clients_are_independent(self, mock_redis):
        limiter = SlidingWindow(limit=3, window_seconds=60)
        for _ in range(3):
            await limiter.check(mock_redis, "user_a", "/api/test")
        # user_a is now at limit — user_b should still be allowed
        result = await limiter.check(mock_redis, "user_b", "/api/test")
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_different_endpoints_are_independent(self, mock_redis):
        limiter = SlidingWindow(limit=3, window_seconds=60)
        for _ in range(3):
            await limiter.check(mock_redis, "user_1", "/api/search")
        # /api/search is at limit — /api/data should still be allowed
        result = await limiter.check(mock_redis, "user_1", "/api/data")
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_retry_after_is_set_when_denied(self, mock_redis):
        limiter = SlidingWindow(limit=2, window_seconds=60)
        await limiter.check(mock_redis, "user_1", "/api/test")
        await limiter.check(mock_redis, "user_1", "/api/test")
        result = await limiter.check(mock_redis, "user_1", "/api/test")
        assert result.allowed is False
        assert result.retry_after is not None
        assert result.retry_after > 0

    @pytest.mark.asyncio
    async def test_requests_in_window_count_is_accurate(self, mock_redis):
        limiter = SlidingWindow(limit=10, window_seconds=60)
        for i in range(4):
            result = await limiter.check(mock_redis, "user_1", "/api/test")
        assert result.requests_in_window == 4


# ── Token Bucket Tests ─────────────────────────────────────────────────────────

class TestTokenBucket:

    @pytest.fixture
    def tb_redis(self):
        """Separate mock Redis for token bucket (uses get/set, not sorted sets)."""
        store = {}
        redis = AsyncMock()

        async def fake_eval(script, num_keys, *args):
            token_key = args[0]
            refill_key = args[1]
            now = float(args[2])
            capacity = float(args[3])
            refill_rate = float(args[4])

            tokens = float(store.get(token_key, capacity))
            last_refill = float(store.get(refill_key, now))

            elapsed = now - last_refill
            new_tokens = min(capacity, tokens + elapsed * refill_rate)

            allowed = 0
            if new_tokens >= 1:
                new_tokens -= 1
                allowed = 1

            store[token_key] = new_tokens
            store[refill_key] = now
            return [allowed, new_tokens]

        async def fake_get(key):
            val = store.get(key)
            return str(val) if val is not None else None

        redis.eval = fake_eval
        redis.get = fake_get
        return redis, store

    @pytest.mark.asyncio
    async def test_allows_up_to_capacity(self, tb_redis):
        redis, _ = tb_redis
        limiter = TokenBucket(capacity=5, refill_rate=1.0)
        for i in range(5):
            result = await limiter.check(redis, "user_1", "/api/test")
            assert result.allowed is True, f"Request {i+1} should be allowed"

    @pytest.mark.asyncio
    async def test_denies_when_bucket_empty(self, tb_redis):
        redis, _ = tb_redis
        limiter = TokenBucket(capacity=3, refill_rate=1.0)
        for _ in range(3):
            await limiter.check(redis, "user_1", "/api/test")
        result = await limiter.check(redis, "user_1", "/api/test")
        assert result.allowed is False

    @pytest.mark.asyncio
    async def test_tokens_remaining_decrements(self, tb_redis):
        redis, _ = tb_redis
        limiter = TokenBucket(capacity=10, refill_rate=1.0)
        result1 = await limiter.check(redis, "user_1", "/api/test")
        result2 = await limiter.check(redis, "user_1", "/api/test")
        assert result2.tokens_remaining < result1.tokens_remaining

    @pytest.mark.asyncio
    async def test_retry_after_set_when_denied(self, tb_redis):
        redis, _ = tb_redis
        limiter = TokenBucket(capacity=1, refill_rate=0.5)
        await limiter.check(redis, "user_1", "/api/test")
        result = await limiter.check(redis, "user_1", "/api/test")
        assert result.allowed is False
        assert result.retry_after is not None
        assert result.retry_after > 0


# ── Config Store Tests ─────────────────────────────────────────────────────────

class TestConfigStore:

    @pytest.mark.asyncio
    async def test_set_and_get_config(self, mock_redis):
        await set_config(mock_redis, "user_1", "/api/search", "token_bucket", 50, 30)
        config = await get_config(mock_redis, "user_1", "/api/search")
        assert config["algorithm"] == "token_bucket"
        assert config["limit"] == 50
        assert config["window_seconds"] == 30

    @pytest.mark.asyncio
    async def test_get_config_returns_defaults_when_not_set(self, mock_redis):
        config = await get_config(mock_redis, "new_user", "/api/unknown")
        assert "algorithm" in config
        assert "limit" in config
        assert "window_seconds" in config

    @pytest.mark.asyncio
    async def test_delete_config(self, mock_redis):
        await set_config(mock_redis, "user_1", "/api/search", "sliding_window", 100, 60)
        deleted = await delete_config(mock_redis, "user_1", "/api/search")
        assert deleted is True
        # After delete, should return defaults (not the custom config)
        config = await get_config(mock_redis, "user_1", "/api/search")
        assert deleted is True  # config was deleted successfully


# ── API Endpoint Tests ─────────────────────────────────────────────────────────

class TestAPI:

    @pytest.mark.asyncio
    async def test_health_endpoint(self, client):
        with patch("app.main.get_redis") as mock_get_redis:
            mock_r = AsyncMock()
            mock_r.ping = AsyncMock(return_value=True)
            mock_get_redis.return_value = mock_r
            response = await client.get("/health")
            assert response.status_code == 200
            assert response.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_check_endpoint_returns_correct_fields(self, client):
        with patch("app.main.get_redis") as mock_get_redis:
            mock_r = AsyncMock()
            mock_get_redis.return_value = mock_r

            with patch("app.main.get_config") as mock_config, \
                 patch("app.main.SlidingWindow") as MockSW:

                mock_config.return_value = {
                    "algorithm": "sliding_window", "limit": 100, "window_seconds": 60
                }
                mock_instance = AsyncMock()
                mock_instance.check.return_value = MagicMock(
                    allowed=True, requests_in_window=1, retry_after=None
                )
                MockSW.return_value = mock_instance

                response = await client.post("/check", json={
                    "client_id": "test_user", "endpoint": "/api/test"
                })
                assert response.status_code == 200
                data = response.json()
                assert "allowed" in data
                assert "client_id" in data
                assert "message" in data

    @pytest.mark.asyncio
    async def test_config_rejects_invalid_algorithm(self, client):
        with patch("app.main.get_redis") as mock_get_redis:
            mock_get_redis.return_value = AsyncMock()
            response = await client.post("/config", json={
                "client_id": "user_1",
                "endpoint": "/api/test",
                "algorithm": "invalid_algo",
                "limit": 100,
                "window_seconds": 60,
            })
            assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_check_requires_client_id(self, client):
        response = await client.post("/check", json={"endpoint": "/api/test"})
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_rate_limit_headers_present(self, client):
        with patch("app.main.get_redis") as mock_get_redis, \
             patch("app.main.get_config") as mock_config, \
             patch("app.main.SlidingWindow") as MockSW:

            mock_get_redis.return_value = AsyncMock()
            mock_config.return_value = {
                "algorithm": "sliding_window", "limit": 100, "window_seconds": 60
            }
            mock_instance = AsyncMock()
            mock_instance.check.return_value = MagicMock(
                allowed=True, requests_in_window=1, retry_after=None
            )
            MockSW.return_value = mock_instance

            response = await client.post("/check", json={
                "client_id": "user_1", "endpoint": "/api/test"
            })
            assert "x-ratelimit-limit" in response.headers
            assert "x-ratelimit-remaining" in response.headers
