from __future__ import annotations
import os
import redis

_pool: redis.ConnectionPool | None = None


def get_redis() -> redis.Redis:
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
        )
    return redis.Redis(connection_pool=_pool)


def dedup_check(key: str, ttl_seconds: int = 600) -> bool:
    """Returns True if this is a new event (not a duplicate). Sets the key if new."""
    r = get_redis()
    result = r.set(key, "1", ex=ttl_seconds, nx=True)
    return result is True


def dedup_key(consumer_group: str, classification: str, topic: str = "") -> str:
    return f"kafka_ops_dedup:{consumer_group}:{classification}:{topic}"
