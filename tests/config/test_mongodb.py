"""Tests for config/mongodb.py"""
import pytest
from unittest.mock import MagicMock, patch
from pymongo.errors import ConnectionFailure


class TestMongoDBConfig:
    def test_config_constants_set(self):
        from config.mongodb import MongoDBConfig
        assert MongoDBConfig.MAX_POOL_SIZE == 100
        assert MongoDBConfig.MIN_POOL_SIZE == 10
        assert MongoDBConfig.CONNECT_TIMEOUT_MS == 5000
        assert MongoDBConfig.SERVER_SELECTION_TIMEOUT_MS == 5000


class TestMongoDBGetClient:
    def setup_method(self):
        from config.mongodb import MongoDB
        MongoDB._client = None
        MongoDB._db = None

    def test_get_client_creates_new_client(self):
        mock_client = MagicMock()
        mock_client.server_info.return_value = {}

        with patch('config.mongodb.MongoClient', return_value=mock_client):
            from config.mongodb import MongoDB
            client = MongoDB.get_client()

        assert client is mock_client

    def test_get_client_reuses_existing_client(self):
        mock_client = MagicMock()

        from config.mongodb import MongoDB
        MongoDB._client = mock_client

        with patch('config.mongodb.MongoClient') as mock_constructor:
            client = MongoDB.get_client()

        mock_constructor.assert_not_called()
        assert client is mock_client

    def test_get_client_raises_on_connection_failure(self):
        mock_client = MagicMock()
        mock_client.server_info.side_effect = ConnectionFailure("Cannot connect")

        from config.mongodb import MongoDB
        with patch('config.mongodb.MongoClient', return_value=mock_client):
            with pytest.raises(ConnectionFailure):
                MongoDB.get_client()

    def test_get_client_passes_correct_args(self):
        mock_client = MagicMock()
        mock_client.server_info.return_value = {}

        from config.mongodb import MongoDB, MongoDBConfig
        with patch('config.mongodb.MongoClient', return_value=mock_client) as mock_constructor:
            MongoDB.get_client()

        call_kwargs = mock_constructor.call_args[1]
        assert call_kwargs['maxPoolSize'] == MongoDBConfig.MAX_POOL_SIZE
        assert call_kwargs['minPoolSize'] == MongoDBConfig.MIN_POOL_SIZE
        assert call_kwargs['tls'] is True
        assert call_kwargs['tlsAllowInvalidCertificates'] is True


class TestMongoDBGetDb:
    def setup_method(self):
        from config.mongodb import MongoDB
        MongoDB._client = None
        MongoDB._db = None

    def test_get_db_creates_db_from_client(self):
        mock_db = MagicMock()
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client.server_info.return_value = {}

        from config.mongodb import MongoDB
        with patch('config.mongodb.MongoClient', return_value=mock_client):
            db = MongoDB.get_db()

        assert db is not None

    def test_get_db_reuses_existing_db(self):
        mock_db = MagicMock()

        from config.mongodb import MongoDB
        MongoDB._db = mock_db

        db = MongoDB.get_db()
        assert db is mock_db


class TestMongoDBCloseConnection:
    def setup_method(self):
        from config.mongodb import MongoDB
        MongoDB._client = None
        MongoDB._db = None

    def test_close_connection_closes_and_clears(self):
        mock_client = MagicMock()
        from config.mongodb import MongoDB
        MongoDB._client = mock_client
        MongoDB._db = MagicMock()

        MongoDB.close_connection()

        mock_client.close.assert_called_once()
        assert MongoDB._client is None
        assert MongoDB._db is None

    def test_close_connection_no_op_when_no_client(self):
        from config.mongodb import MongoDB
        MongoDB._client = None
        # Should not raise
        MongoDB.close_connection()
