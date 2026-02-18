"""
Distributed Rate Limiter Service
---------------------------------
FastAPI app exposing rate limiting as a standalone REST API.
Any service can call /check before processing a request to enforce limits.

Production features:
- Redis failure handling with fail-open fallback
- In-memory config cache (5s TTL) to reduce Redis round-trips
- Structured JSON logging on every request
- In-memory metrics (total, allowed, denied, redis failures)
"""

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import time
import asyncio

from app.redis_client import get_redis, close_redis
from app.config_store import get_config, set_config, delete_config
from app.config_cache import cache_get, cache_set, cache_invalidate, cache_stats
from app.algorithms.token_bucket import TokenBucket
from app.algorithms.sliding_window import SlidingWindow
from app.logger import logger
from app.metrics import metrics

REDIS_TIMEOUT = float(2.0)
FAIL_OPEN = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        redis = await get_redis()
        await asyncio.wait_for(redis.ping(), timeout=REDIS_TIMEOUT)
        logger.info("Redis connection established")
    except Exception as e:
        logger.warning(f"Redis not reachable on startup: {e}. Will retry on requests.")
    yield
    await close_redis()
    logger.info("Redis connection closed")


app = FastAPI(
    title="Distributed Rate Limiter Service",
    description="Configurable rate limiting with Token Bucket and Sliding Window. Backed by Redis with fail-open fallback.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Models ────────────────────────────────────────────────────────────────────

class CheckRequest(BaseModel):
    client_id: str = Field(..., example="user_42")
    endpoint: str = Field(..., example="/api/search")


class CheckResponse(BaseModel):
    allowed: bool
    client_id: str
    endpoint: str
    algorithm: str
    limit: int
    window_seconds: int | None = None
    requests_in_window: int | None = None
    tokens_remaining: float | None = None
    retry_after: float | None = None
    redis_available: bool = True
    message: str


class ConfigRequest(BaseModel):
    client_id: str = Field(..., example="user_42")
    endpoint: str = Field(..., example="/api/search")
    algorithm: str = Field(..., example="sliding_window")
    limit: int = Field(..., gt=0, example=100)
    window_seconds: int = Field(..., gt=0, example=60)


class StatsResponse(BaseModel):
    client_id: str
    endpoint: str
    algorithm: str
    limit: int
    window_seconds: int
    current_usage: int | None = None
    tokens_remaining: float | None = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _allow_on_failure(client_id: str, endpoint: str, error: Exception) -> CheckResponse:
    """
    Fallback when Redis is unavailable.
    Fail-open: allow request, log and record the failure.
    This prevents the rate limiter becoming a single point of failure.
    """
    metrics.record_redis_failure()
    metrics.record(endpoint, was_allowed=FAIL_OPEN)

    logger.warning(
        "Redis unavailable — applying fallback",
        extra={
            "client_id": client_id,
            "endpoint": endpoint,
            "fallback": "fail_open" if FAIL_OPEN else "fail_closed",
            "error": str(error),
        }
    )

    if FAIL_OPEN:
        return CheckResponse(
            allowed=True,
            client_id=client_id,
            endpoint=endpoint,
            algorithm="none",
            limit=0,
            redis_available=False,
            message="Rate limiter degraded (Redis unavailable). Request allowed via fail-open policy.",
        )
    else:
        return CheckResponse(
            allowed=False,
            client_id=client_id,
            endpoint=endpoint,
            algorithm="none",
            limit=0,
            redis_available=False,
            message="Rate limiter degraded (Redis unavailable). Request denied via fail-closed policy.",
        )


async def _get_config_cached(redis, client_id: str, endpoint: str) -> dict:
    """
    Config lookup with in-memory cache.
    Cache hit  → return immediately, zero Redis calls.
    Cache miss → fetch from Redis, populate cache for TTL seconds.
    Cuts Redis calls per /check from 2 → 1 for cached clients.
    """
    cached = cache_get(client_id, endpoint)
    if cached is not None:
        return cached

    config = await asyncio.wait_for(
        get_config(redis, client_id, endpoint),
        timeout=REDIS_TIMEOUT
    )
    cache_set(client_id, endpoint, config)
    return config


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post("/check", response_model=CheckResponse, tags=["Rate Limiting"])
async def check_rate_limit(req: CheckRequest, response: Response):
    """
    Core endpoint. Call before processing any incoming request.
    Gracefully degrades if Redis is unavailable (fail-open by default).
    """
    try:
        redis = await get_redis()
        config = await _get_config_cached(redis, req.client_id, req.endpoint)

        algorithm = config["algorithm"]
        limit = config["limit"]
        window_seconds = config["window_seconds"]

        if algorithm == "token_bucket":
            limiter = TokenBucket(capacity=limit, refill_rate=limit / window_seconds)
            result = await asyncio.wait_for(
                limiter.check(redis, req.client_id, req.endpoint),
                timeout=REDIS_TIMEOUT
            )

            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(int(result.tokens_remaining))
            response.headers["X-RateLimit-Algorithm"] = "token_bucket"
            if result.retry_after:
                response.headers["Retry-After"] = str(result.retry_after)

            metrics.record(req.endpoint, was_allowed=result.allowed)
            logger.info("check", extra={
                "client_id": req.client_id, "endpoint": req.endpoint,
                "algorithm": "token_bucket", "allowed": result.allowed,
                "tokens_remaining": result.tokens_remaining,
            })

            return CheckResponse(
                allowed=result.allowed, client_id=req.client_id,
                endpoint=req.endpoint, algorithm="token_bucket",
                limit=limit, window_seconds=window_seconds,
                tokens_remaining=result.tokens_remaining,
                retry_after=result.retry_after,
                message="Request allowed." if result.allowed else f"Rate limit exceeded. Retry after {result.retry_after}s.",
            )

        else:  # sliding_window
            limiter = SlidingWindow(limit=limit, window_seconds=window_seconds)
            result = await asyncio.wait_for(
                limiter.check(redis, req.client_id, req.endpoint),
                timeout=REDIS_TIMEOUT
            )

            reset_time = int(time.time()) + (result.retry_after or window_seconds)
            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(max(0, limit - result.requests_in_window))
            response.headers["X-RateLimit-Reset"] = str(reset_time)
            response.headers["X-RateLimit-Algorithm"] = "sliding_window"
            if result.retry_after:
                response.headers["Retry-After"] = str(result.retry_after)

            metrics.record(req.endpoint, was_allowed=result.allowed)
            logger.info("check", extra={
                "client_id": req.client_id, "endpoint": req.endpoint,
                "algorithm": "sliding_window", "allowed": result.allowed,
                "requests_in_window": result.requests_in_window,
            })

            return CheckResponse(
                allowed=result.allowed, client_id=req.client_id,
                endpoint=req.endpoint, algorithm="sliding_window",
                limit=limit, window_seconds=window_seconds,
                requests_in_window=result.requests_in_window,
                retry_after=result.retry_after,
                message="Request allowed." if result.allowed else f"Rate limit exceeded. Retry after {result.retry_after}s.",
            )

    except asyncio.TimeoutError:
        return _allow_on_failure(req.client_id, req.endpoint, Exception("Redis timeout"))
    except Exception as e:
        return _allow_on_failure(req.client_id, req.endpoint, e)


@app.post("/config", tags=["Configuration"])
async def set_rate_limit_config(req: ConfigRequest):
    """
    Set a custom rate limit rule for a specific client + endpoint.
    Immediately invalidates the config cache for this client.
    """
    if req.algorithm not in ("token_bucket", "sliding_window"):
        raise HTTPException(status_code=400, detail="algorithm must be 'token_bucket' or 'sliding_window'")

    try:
        redis = await get_redis()
        config = await asyncio.wait_for(
            set_config(redis, req.client_id, req.endpoint, req.algorithm, req.limit, req.window_seconds),
            timeout=REDIS_TIMEOUT
        )
        cache_invalidate(req.client_id, req.endpoint)
        logger.info("config_set", extra={"client_id": req.client_id, "endpoint": req.endpoint})
        return {
            "message": f"Config saved for client '{req.client_id}' on endpoint '{req.endpoint}'.",
            "config": config,
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {str(e)}")


@app.delete("/config", tags=["Configuration"])
async def delete_rate_limit_config(client_id: str, endpoint: str):
    """Delete custom config. Client falls back to global defaults."""
    try:
        redis = await get_redis()
        deleted = await asyncio.wait_for(
            delete_config(redis, client_id, endpoint),
            timeout=REDIS_TIMEOUT
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="No custom config found.")
        cache_invalidate(client_id, endpoint)
        logger.info("config_deleted", extra={"client_id": client_id, "endpoint": endpoint})
        return {"message": f"Config deleted. '{client_id}' on '{endpoint}' now uses global defaults."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {str(e)}")


@app.get("/stats/{client_id}", response_model=StatsResponse, tags=["Observability"])
async def get_stats(client_id: str, endpoint: str = "/"):
    """View current rate limit usage for a client."""
    try:
        redis = await get_redis()
        config = await _get_config_cached(redis, client_id, endpoint)
        algorithm = config["algorithm"]
        limit = config["limit"]
        window_seconds = config["window_seconds"]

        if algorithm == "sliding_window":
            key = f"rl:sw:{client_id}:{endpoint}"
            window_start = time.time() - window_seconds
            await redis.zremrangebyscore(key, "-inf", window_start)
            count = await redis.zcard(key)
            return StatsResponse(
                client_id=client_id, endpoint=endpoint,
                algorithm=algorithm, limit=limit,
                window_seconds=window_seconds, current_usage=count,
            )
        else:
            token_key = f"rl:tb:{client_id}:{endpoint}:tokens"
            tokens = await redis.get(token_key)
            return StatsResponse(
                client_id=client_id, endpoint=endpoint,
                algorithm=algorithm, limit=limit,
                window_seconds=window_seconds,
                tokens_remaining=float(tokens) if tokens else float(limit),
            )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {str(e)}")


@app.get("/metrics", tags=["Observability"])
async def get_metrics():
    """
    Service-level metrics: total requests, allowed vs denied,
    Redis failure count, per-endpoint breakdown, and cache stats.
    """
    return {
        **metrics.summary(),
        "cache": cache_stats(),
    }


@app.get("/health", tags=["Health"])
async def health():
    """Redis connectivity and service health check."""
    try:
        redis = await get_redis()
        await asyncio.wait_for(redis.ping(), timeout=REDIS_TIMEOUT)
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        return {"status": "degraded", "redis": "unavailable", "error": str(e)}
