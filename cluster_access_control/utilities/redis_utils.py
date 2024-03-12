from typing import Final

import redis

TEST_AND_SET_LUA_SCRIPT: Final[str] = """
if redis.call("exists", KEYS[1]) == 1 then
    return false
else
    return redis.call("setex", KEYS[1], ARGV[2], ARGV[1])
end
"""


def redis_test_and_set(redis_client: redis.Redis, key: str, timeout: int) -> bool:
    return redis_client.eval(TEST_AND_SET_LUA_SCRIPT, 1, key, 1, timeout)
