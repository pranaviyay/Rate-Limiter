"""
Config Cache
------------
In-memory LRU-style cache for rate limit configs.
Prevents a Redis round-trip on every /check call for config lookup.

Strategy:
- Cache TTL: 5 seconds (configurable)
- On cache miss: fetch from Redis, store in cache
- On config update (POST /config): invalidate that entry immediately
- Thread-safe for async: asyncio is single-threaded, no locks needed

Impact: reduces Redis calls per /check from 2 to 1 for cached clients.
"""

import time
from typing import Dict, Tuple, Optional
import os

CACHE_TTL = int(os.getenv("CONFIG_CACHE_TTL", 5))  # seconds

# Cache structure: key -> (config_dict, expiry_timestamp)
_cache: Dict[str, Tuple[dict, float]] = {}


def _cache_key(client_id: str, endpoint: str) -> str:
    return f"{client_id}::{endpoint}"


def cache_get(client_id: str, endpoint: str) -> Optional[dict]:
    """Return cached config if present and not expired, else None."""
    key = _cache_key(client_id, endpoint)
    entry = _cache.get(key)
    if entry is None:
        return None
    config, expiry = entry
    if time.time() > expiry:
        del _cache[key]
        return None
    return config


def cache_set(client_id: str, endpoint: str, config: dict):
    """Store config in cache with TTL."""
    key = _cache_key(client_id, endpoint)
    _cache[key] = (config, time.time() + CACHE_TTL)


def cache_invalidate(client_id: str, endpoint: str):
    """Remove a specific entry — call this after POST /config or DELETE /config."""
    key = _cache_key(client_id, endpoint)
    _cache.pop(key, None)


def cache_stats() -> dict:
    """How many entries are currently cached (including expired)."""
    now = time.time()
    active = sum(1 for _, (_, exp) in _cache.items() if exp > now)
    return {"cached_configs": active, "ttl_seconds": CACHE_TTL}
