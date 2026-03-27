import redis
from config.settings import Config


class RedisClient:
    def __init__(self):
        pool = redis.ConnectionPool(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB,
            password=Config.REDIS_PASSWORD,
            decode_responses=True
        )
        self.redis = redis.StrictRedis(connection_pool=pool)

    def safe_execute(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.RedisError as e:
            print(f"[Redis Error] {e}")
            return None

    # Strings
    def set(self, key, value):
        return self.safe_execute(self.redis.set, key, value)

    def get(self, key):
        return self.safe_execute(self.redis.get, key)

    def delete(self, key):
        return self.safe_execute(self.redis.delete, key)

    def exists(self, key):
        return self.safe_execute(self.redis.exists, key)

    def expire(self, key, timeout):
        return self.safe_execute(self.redis.expire, key, timeout)

    def ttl(self, key):
        return self.safe_execute(self.redis.ttl, key)

    # Hashes
    def hset(self, name, key, value):
        return self.safe_execute(self.redis.hset, name, key, value)

    def hget(self, name, key):
        return self.safe_execute(self.redis.hget, name, key)

    def hgetall(self, name):
        return self.safe_execute(self.redis.hgetall, name)

    # Pipeline
    def pipeline(self):
        return self.redis.pipeline()

    def ping(self):
        return self.safe_execute(self.redis.ping)

    def close(self):
        try:
            self.redis.connection_pool.disconnect()
        except redis.RedisError as e:
            print(f"[Redis Close Error] {e}")
            return None


redis = RedisClient()
