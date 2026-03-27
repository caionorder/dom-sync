"""Tests for client/redis.py

Note: client/redis.py assigns `redis = RedisClient()` at module level, which
shadows the `redis` library name in the module's namespace. To instantiate a
new RedisClient during tests, we temporarily restore the module-level `redis`
attribute back to the real redis library (with ConnectionPool / StrictRedis
patched), then reset it afterward.
"""
import pytest
from unittest.mock import MagicMock, patch
import redis as redis_lib


class _RedisModuleRestored:
    """Context manager: temporarily restores 'redis' in client.redis module to the library."""

    def __enter__(self):
        import client.redis as redis_module
        self._module = redis_module
        self._original = redis_module.redis
        redis_module.redis = redis_lib
        return redis_module

    def __exit__(self, *args):
        self._module.redis = self._original


def make_client():
    """Return a RedisClient with the internal self.redis replaced by a MagicMock."""
    with _RedisModuleRestored() as redis_module:
        mock_strict = MagicMock()
        with patch.object(redis_lib, 'ConnectionPool', return_value=MagicMock()), \
             patch.object(redis_lib, 'StrictRedis', return_value=mock_strict):
            client = redis_module.RedisClient()
    # Keep the module pointing to redis_lib so that RedisError references work
    # during test execution
    import client.redis as m
    m.redis = redis_lib
    client.redis = MagicMock()
    return client


class TestRedisClientInit:
    def test_init_creates_connection_pool_and_strict_redis(self):
        import client.redis as redis_module
        original_redis = redis_module.redis
        redis_module.redis = redis_lib

        mock_pool = MagicMock()
        mock_strict = MagicMock()
        try:
            with patch.object(redis_lib, 'ConnectionPool', return_value=mock_pool) as pool_cls, \
                 patch.object(redis_lib, 'StrictRedis', return_value=mock_strict):
                client = redis_module.RedisClient()
        finally:
            redis_module.redis = original_redis

        pool_cls.assert_called_once()
        assert client.redis is mock_strict

    def test_init_passes_decode_responses(self):
        import client.redis as redis_module
        original_redis = redis_module.redis
        redis_module.redis = redis_lib

        captured = {}

        def fake_pool(**kwargs):
            captured.update(kwargs)
            return MagicMock()

        try:
            with patch.object(redis_lib, 'ConnectionPool', side_effect=fake_pool), \
                 patch.object(redis_lib, 'StrictRedis', return_value=MagicMock()):
                redis_module.RedisClient()
        finally:
            redis_module.redis = original_redis

        assert captured.get('decode_responses') is True


class TestRedisClientSafeExecute:
    def setup_method(self):
        self.client = make_client()

    def test_safe_execute_returns_func_result(self):
        fn = MagicMock(return_value='value')
        result = self.client.safe_execute(fn, 'key')
        assert result == 'value'

    def test_safe_execute_returns_none_on_redis_error(self):
        fn = MagicMock(side_effect=redis_lib.RedisError("connection refused"))
        result = self.client.safe_execute(fn, 'key')
        assert result is None

    def test_safe_execute_prints_error_message(self, capsys):
        fn = MagicMock(side_effect=redis_lib.RedisError("timeout"))
        self.client.safe_execute(fn, 'key')
        assert '[Redis Error]' in capsys.readouterr().out


class TestRedisClientStringOps:
    def setup_method(self):
        self.client = make_client()

    def test_set_calls_redis_set(self):
        self.client.redis.set.return_value = True
        result = self.client.set('key', 'value')
        self.client.redis.set.assert_called_once_with('key', 'value')
        assert result is True

    def test_get_calls_redis_get(self):
        self.client.redis.get.return_value = 'cached_value'
        result = self.client.get('key')
        self.client.redis.get.assert_called_once_with('key')
        assert result == 'cached_value'

    def test_delete_calls_redis_delete(self):
        self.client.redis.delete.return_value = 1
        result = self.client.delete('key')
        self.client.redis.delete.assert_called_once_with('key')
        assert result == 1

    def test_exists_calls_redis_exists(self):
        self.client.redis.exists.return_value = 1
        result = self.client.exists('key')
        self.client.redis.exists.assert_called_once_with('key')
        assert result == 1

    def test_expire_calls_redis_expire(self):
        self.client.redis.expire.return_value = True
        result = self.client.expire('key', 3600)
        self.client.redis.expire.assert_called_once_with('key', 3600)
        assert result is True

    def test_ttl_calls_redis_ttl(self):
        self.client.redis.ttl.return_value = 1800
        result = self.client.ttl('key')
        self.client.redis.ttl.assert_called_once_with('key')
        assert result == 1800


class TestRedisClientHashOps:
    def setup_method(self):
        self.client = make_client()

    def test_hset_calls_redis_hset(self):
        self.client.redis.hset.return_value = 1
        result = self.client.hset('myhash', 'field', 'value')
        self.client.redis.hset.assert_called_once_with('myhash', 'field', 'value')
        assert result == 1

    def test_hget_calls_redis_hget(self):
        self.client.redis.hget.return_value = 'value'
        result = self.client.hget('myhash', 'field')
        self.client.redis.hget.assert_called_once_with('myhash', 'field')
        assert result == 'value'

    def test_hgetall_calls_redis_hgetall(self):
        self.client.redis.hgetall.return_value = {'field': 'value'}
        result = self.client.hgetall('myhash')
        self.client.redis.hgetall.assert_called_once_with('myhash')
        assert result == {'field': 'value'}


class TestRedisClientPipeline:
    def setup_method(self):
        self.client = make_client()

    def test_pipeline_returns_redis_pipeline(self):
        mock_pipeline = MagicMock()
        self.client.redis.pipeline.return_value = mock_pipeline
        result = self.client.pipeline()
        self.client.redis.pipeline.assert_called_once()
        assert result is mock_pipeline


class TestRedisClientPingClose:
    def setup_method(self):
        self.client = make_client()

    def test_ping_calls_redis_ping(self):
        self.client.redis.ping.return_value = True
        result = self.client.ping()
        self.client.redis.ping.assert_called_once()
        assert result is True

    def test_close_disconnects_pool(self):
        mock_pool = MagicMock()
        self.client.redis.connection_pool = mock_pool
        self.client.close()
        mock_pool.disconnect.assert_called_once()

    def test_close_returns_none_on_redis_error(self):
        mock_pool = MagicMock()
        mock_pool.disconnect.side_effect = redis_lib.RedisError("pool error")
        self.client.redis.connection_pool = mock_pool
        result = self.client.close()
        assert result is None

    def test_close_prints_error_on_redis_error(self, capsys):
        mock_pool = MagicMock()
        mock_pool.disconnect.side_effect = redis_lib.RedisError("pool error")
        self.client.redis.connection_pool = mock_pool
        self.client.close()
        assert '[Redis Close Error]' in capsys.readouterr().out
