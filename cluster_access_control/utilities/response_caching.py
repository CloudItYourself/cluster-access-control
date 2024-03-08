import aioredis
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis.client import Redis

from cluster_access_control.utilities.environment import ClusterAccessConfiguration


async def initialize_fastapi_cache():
    environment = ClusterAccessConfiguration()
    redis_client = aioredis.from_url(f"{environment.get_redis_url()}/0")
    FastAPICache.init(RedisBackend(redis_client), prefix="cluster-access-control-fastapi-cache")
