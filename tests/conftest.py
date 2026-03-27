"""Shared fixtures for DOM test suite."""
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture(autouse=True)
def reset_config_singleton():
    """Reset ConfigSingleton between tests to prevent state leakage."""
    from config import settings
    original_instance = settings.ConfigSingleton._instance
    yield
    settings.ConfigSingleton._instance = original_instance


@pytest.fixture(autouse=True)
def reset_mongodb_singleton():
    """Reset MongoDB class-level state between tests."""
    from config import mongodb
    original_client = mongodb.MongoDB._client
    original_db = mongodb.MongoDB._db
    yield
    mongodb.MongoDB._client = original_client
    mongodb.MongoDB._db = original_db


@pytest.fixture(autouse=True)
def reset_rate_limiter_singleton():
    """Reset NetworkRateLimiter singleton between tests."""
    from utils import network_rate_limiter as nrl_module
    original_instance = nrl_module.NetworkRateLimiter._instance
    yield
    nrl_module.NetworkRateLimiter._instance = original_instance
    if nrl_module.NetworkRateLimiter._instance is not None:
        if hasattr(nrl_module.NetworkRateLimiter._instance, '_initialized'):
            del nrl_module.NetworkRateLimiter._instance._initialized


@pytest.fixture
def mock_mongo_collection():
    """Returns a MagicMock that simulates a pymongo collection."""
    collection = MagicMock()
    bulk_result = MagicMock()
    bulk_result.matched_count = 1
    bulk_result.modified_count = 1
    bulk_result.upserted_count = 0
    collection.bulk_write.return_value = bulk_result
    return collection


@pytest.fixture
def mock_mongo_db(mock_mongo_collection):
    """Returns a MagicMock that simulates a pymongo database."""
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=mock_mongo_collection)
    return db


@pytest.fixture
def mock_mongodb_get_db(mock_mongo_db):
    """Patches MongoDB.get_db to return a mock database."""
    with patch('config.mongodb.MongoDB.get_db', return_value=mock_mongo_db):
        yield mock_mongo_db


@pytest.fixture
def mock_redis_client():
    """Returns a MagicMock for the redis client."""
    client = MagicMock()
    client.get.return_value = None
    client.set.return_value = True
    client.delete.return_value = 1
    client.exists.return_value = 0
    client.expire.return_value = True
    client.ttl.return_value = -1
    client.ping.return_value = True
    return client
