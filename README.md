# Distributed Rate Limiter Service

A standalone backend service that enforces configurable rate limits across distributed API deployments. Any service can call `/check` before processing a request — the limiter responds with allow/deny based on per-client, per-endpoint rules stored in Redis.

Built to work correctly even when the consuming application runs on multiple servers simultaneously — all state is stored atomically in Redis.

---

## Architecture

```
Client App (any language)
        │
        │  POST /check {client_id, endpoint}
        ▼
┌──────────────────────┐
│   Rate Limiter API   │  FastAPI + Uvicorn
│   (this service)     │
└──────────┬───────────┘
           │  Atomic Lua scripts
           ▼
┌──────────────────────┐
│        Redis         │  Sorted Sets (sliding window)
│                      │  Strings (token bucket)
└──────────────────────┘
```

Deployed via Docker Compose — single command to spin up app + Redis.

---

## Algorithms

### Sliding Window Counter (default)
Tracks exact request timestamps in a Redis Sorted Set. On each request:
1. Prune entries older than the window
2. Count remaining — if under limit, allow and add current timestamp
3. Set TTL so the key auto-expires

**Tradeoff:** Accurate and fair. No burst allowance. Higher memory use (one entry per request).

### Token Bucket
Each client gets a virtual bucket of N tokens. Each request costs 1 token. Tokens refill at a fixed rate. Empty bucket → denied.

**Tradeoff:** Allows short bursts (if bucket is full). Lower memory (2 keys per client). Slightly more complex refill logic.

### Why Lua scripts?
Both algorithms use Redis Lua scripts for their core logic. Lua scripts execute atomically in Redis — meaning the entire read-compute-write sequence happens without interruption, even across multiple app instances. This prevents race conditions where two servers might both read "99 requests" and both allow request 100, resulting in 101 actual requests.

---

## API Reference

### `POST /check`
Core endpoint — call before processing any incoming request.

**Request:**
```json
{ "client_id": "user_42", "endpoint": "/api/search" }
```

**Response:**
```json
{
  "allowed": true,
  "client_id": "user_42",
  "endpoint": "/api/search",
  "algorithm": "sliding_window",
  "limit": 100,
  "window_seconds": 60,
  "requests_in_window": 43,
  "retry_after": null,
  "message": "Request allowed."
}
```

**Headers returned:**
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 57
X-RateLimit-Reset: 1714394460
X-RateLimit-Algorithm: sliding_window
```

---

### `POST /config`
Set custom limits per client + endpoint. Overrides global defaults.

```json
{
  "client_id": "user_42",
  "endpoint": "/api/export",
  "algorithm": "token_bucket",
  "limit": 10,
  "window_seconds": 60
}
```

---

### `GET /stats/{client_id}?endpoint=/api/search`
View current usage for any client.

---

### `DELETE /config?client_id=user_42&endpoint=/api/search`
Remove custom config. Client falls back to global defaults.

---

### `GET /health`
Redis connectivity check.

---

## Quick Start

```bash
# Clone and start
git clone <repo>
cd ratelimiter
docker-compose up --build

# Service available at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

---

## Configuration

Edit `.env` to set global defaults:

```env
DEFAULT_ALGORITHM=sliding_window   # or token_bucket
DEFAULT_LIMIT=100                  # requests per window
DEFAULT_WINDOW=60                  # window size in seconds
```

---

## Benchmarking

```bash
pip install locust
locust -f benchmark/locustfile.py --host=http://localhost:8000
# Open http://localhost:8089
```

### Results (MacBook Air M5, Redis in Docker)

| Scenario | Requests | RPS | Median Latency | p95 Latency | Failures |
|---|---|---|---|---|---|
| Sustained (/check) | 18,101 | 156.9 | 1ms | 3ms | 0% |
| Burst (/check [burst]) | 409,851 | 3,345 | 2ms | 4ms | 0% |
| Mixed (aggregated) | 450,739 | 3,698 | 2ms | 4ms | 0% |

Rate limiter overhead per request: **<5ms across all scenarios**
---

## Algorithm Comparison

| | Sliding Window | Token Bucket |
|---|---|---|
| **Fairness** | High — no edge cases | Medium — burst possible |
| **Memory** | Higher (1 entry/request) | Lower (2 keys/client) |
| **Burst handling** | None | Allowed |
| **Best for** | Backend APIs, auth endpoints | User-facing APIs |
| **Accuracy** | Exact | Approximate (float math) |

---

## Project Structure

```
ratelimiter/
│
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI app — 7 endpoints, Redis failure handling
│   ├── redis_client.py       # Shared async Redis connection
│   ├── config_store.py       # Per-client rule persistence in Redis
│   ├── config_cache.py       # In-memory config cache (5s TTL)
│   ├── logger.py             # Structured JSON logging
│   ├── metrics.py            # In-memory request counters
│   └── algorithms/
│       ├── __init__.py
│       ├── sliding_window.py # Sorted set + Lua atomic script
│       └── token_bucket.py   # Token refill + Lua atomic script
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # sys.path fix for Docker
│   └── test_ratelimiter.py   # 18 unit tests (no real Redis needed)
│
├── benchmark/
│   └── locustfile.py         # 3 load test scenarios
│
├── docker-compose.yml        # App + Redis, one command startup
├── Dockerfile
├── requirements.txt
├── pytest.ini
└── .env                      # Default algorithm, limit, window, cache TTL
```
