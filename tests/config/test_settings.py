"""Tests for config/settings.py"""
import pytest
from unittest.mock import patch


class TestConfigSingleton:
    def test_singleton_returns_same_instance(self):
        from config.settings import ConfigSingleton
        ConfigSingleton._instance = None
        with patch('config.settings.load_dotenv'), \
             patch.dict('os.environ', {
                 'GAM_KEY_FILE': '/tmp/key.json',
                 'REDIS_HOST': 'localhost',
                 'REDIS_PORT': '6379',
                 'REDIS_DB': '0',
                 'MONGO_HOST': 'localhost',
                 'MONGO_PORT': '27017',
                 'MONGO_DB': 'testdb',
                 'MONGO_USER': 'user',
                 'MONGO_PASSWORD': 'pass',
             }, clear=False):
            instance1 = ConfigSingleton()
            instance2 = ConfigSingleton()
        assert instance1 is instance2

    def test_load_config_reads_gam_key_file(self):
        from config.settings import ConfigSingleton
        ConfigSingleton._instance = None
        with patch('config.settings.load_dotenv'), \
             patch.dict('os.environ', {'GAM_KEY_FILE': '/tmp/service_account.json'}, clear=False):
            instance = ConfigSingleton()
        assert instance.json_key_path == '/tmp/service_account.json'

    def test_load_config_redis_defaults(self):
        from config.settings import ConfigSingleton
        ConfigSingleton._instance = None
        env = {}
        with patch('config.settings.load_dotenv'), \
             patch.dict('os.environ', env, clear=True):
            instance = ConfigSingleton()
        assert instance.REDIS_HOST == 'localhost'
        assert instance.REDIS_PORT == 6379
        assert instance.REDIS_DB == 0
        assert instance.REDIS_PASSWORD is None

    def test_load_config_redis_from_env(self):
        from config.settings import ConfigSingleton
        ConfigSingleton._instance = None
        env = {
            'REDIS_HOST': 'redis.example.com',
            'REDIS_PORT': '6380',
            'REDIS_DB': '2',
            'REDIS_PASSWORD': 'secret',
        }
        with patch('config.settings.load_dotenv'), \
             patch.dict('os.environ', env, clear=False):
            instance = ConfigSingleton()
        assert instance.REDIS_HOST == 'redis.example.com'
        assert instance.REDIS_PORT == 6380
        assert instance.REDIS_DB == 2
        assert instance.REDIS_PASSWORD == 'secret'

    def test_load_config_mongo_defaults(self):
        from config.settings import ConfigSingleton
        ConfigSingleton._instance = None
        with patch('config.settings.load_dotenv'), \
             patch.dict('os.environ', {}, clear=True):
            instance = ConfigSingleton()
        assert instance.MONGO_HOST == 'localhost'
        assert instance.MONGO_PORT == 27017
        assert instance.MONGO_DB == 'admanager'
        assert instance.MONGO_USER == 'joinads'
        assert instance.MONGO_PASSWORD == 'joinads'

    def test_load_config_mongo_uri_built_correctly(self):
        from config.settings import ConfigSingleton
        ConfigSingleton._instance = None
        env = {
            'MONGO_USER': 'myuser',
            'MONGO_PASSWORD': 'mypass',
            'MONGO_HOST': 'cluster.mongodb.net',
        }
        with patch('config.settings.load_dotenv'), \
             patch.dict('os.environ', env, clear=False):
            instance = ConfigSingleton()
        assert 'myuser' in instance.MONGODB_URI
        assert 'mypass' in instance.MONGODB_URI
        assert 'cluster.mongodb.net' in instance.MONGODB_URI

    def test_singleton_not_recreated_when_instance_exists(self):
        from config.settings import ConfigSingleton
        ConfigSingleton._instance = None
        with patch('config.settings.load_dotenv') as mock_load, \
             patch.dict('os.environ', {}, clear=True):
            ConfigSingleton()
            ConfigSingleton()
        # load_dotenv should only be called once (inside __new__)
        assert mock_load.call_count == 1
