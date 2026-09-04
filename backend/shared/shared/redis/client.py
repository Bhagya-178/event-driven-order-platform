import logging
from typing import Optional, Any
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

class RedisManager:
    """
    Async Redis Manager with built-in fail-open resilience.
    If Redis is down or unreachable, operations log a warning and return gracefully
    instead of throwing 500 errors to clients.
    """
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.client: Optional[aioredis.Redis] = None

    async def connect(self):
        try:
            self.client = aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=1.0,
                socket_connect_timeout=1.0
            )
            await self.client.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.warning(f"Redis is unavailable at {self.redis_url} (running in fail-open mode): {e}")
            self.client = None

    async def close(self):
        if self.client:
            await self.client.close()
            logger.info("Redis connection closed")

    async def ping(self) -> bool:
        if not self.client:
            return False
        try:
            return await self.client.ping()
        except Exception:
            return False

    async def get(self, key: str) -> Optional[str]:
        if not self.client:
            return None
        try:
            return await self.client.get(key)
        except Exception as e:
            logger.warning(f"Redis GET failed for key '{key}' (fail-open): {e}")
            return None

    async def set(self, key: str, value: str, expire: Optional[int] = None) -> bool:
        if not self.client:
            return False
        try:
            if expire:
                await self.client.setex(key, expire, value)
            else:
                await self.client.set(key, value)
            return True
        except Exception as e:
            logger.warning(f"Redis SET failed for key '{key}' (fail-open): {e}")
            return False

    async def delete(self, key: str) -> bool:
        if not self.client:
            return False
        try:
            await self.client.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Redis DELETE failed for key '{key}' (fail-open): {e}")
            return False

    async def increment(self, key: str, ttl: int = 60) -> int:
        """
        Increments a key and sets TTL if key is new. Used for rate limiting.
        """
        if not self.client:
            return 1  # Fail-open
        try:
            val = await self.client.incr(key)
            if val == 1:
                await self.client.expire(key, ttl)
            return val
        except Exception as e:
            logger.warning(f"Redis INCR failed for key '{key}' (fail-open): {e}")
            return 1
