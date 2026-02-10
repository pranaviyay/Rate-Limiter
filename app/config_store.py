"""
Config Store
------------
Stores per-client and per-endpoint rate limit rules in Redis.
Falls back to defaults from .env if no custom rule is found.

Redis key: rl:config:{client_id}:{endpoint}  → Hash
"""

import json
import os
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

DEFAULT_ALGORITHM = os.getenv("DEFAULT_ALGORITHM", "sliding_window")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", 100))
DEFAULT_WINDOW = int(os.getenv("DEFAULT_WINDOW", 60))

CONFIG_TTL = 86400 * 7  # configs persist for 7 days


async def set_config(
    redis: aioredis.Redis,
    client_id: str,
    endpoint: str,
    algorithm: str,
    limit: int,
    window_seconds: int,
):
    key = f"rl:config:{client_id}:{endpoint}"
    config = {
        "algorithm": algorithm,
        "limit": limit,
        "window_seconds": window_seconds,
    }
    await redis.set(key, json.dumps(config), ex=CONFIG_TTL)
    return config


async def get_config(
    redis: aioredis.Redis,
    client_id: str,
    endpoint: str,
) -> dict:
    key = f"rl:config:{client_id}:{endpoint}"
    raw = await redis.get(key)
    if raw:
        return json.loads(raw)
    # Return defaults if no custom config
    return {
        "algorithm": DEFAULT_ALGORITHM,
        "limit": DEFAULT_LIMIT,
        "window_seconds": DEFAULT_WINDOW,
    }


async def delete_config(
    redis: aioredis.Redis,
    client_id: str,
    endpoint: str,
):
    key = f"rl:config:{client_id}:{endpoint}"
    deleted = await redis.delete(key)
    return bool(deleted)
