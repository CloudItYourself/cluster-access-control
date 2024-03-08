from fastapi_cache import FastAPICache
from redis.asyncio import Redis
from fastapi_cache.backends.redis import RedisBackend
from cluster_access_control.utilities.environment import ClusterAccessConfiguration


async def initialize_fastapi_cache():
    environment = ClusterAccessConfiguration()
    redis_client = Redis.from_url(f"{environment.get_redis_url()}/0")
    FastAPICache.init(RedisBackend(redis_client), prefix="cluster-access-control-fastapi-cache")
