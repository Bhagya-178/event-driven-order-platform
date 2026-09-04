from shared.redis.client import RedisManager
from app.core.config import settings

redis_manager = RedisManager(redis_url=settings.REDIS_URL)
