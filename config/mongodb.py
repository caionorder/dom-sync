from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from config.settings import Config
import logging

logger = logging.getLogger(__name__)


class MongoDBConfig:
    MONGODB_URI = Config.MONGODB_URI
    MONGODB_DB = Config.MONGO_DB

    # Configuracoes de pool de conexao
    MAX_POOL_SIZE = 100
    MIN_POOL_SIZE = 10

    # Timeout de conexao
    CONNECT_TIMEOUT_MS = 5000
    SERVER_SELECTION_TIMEOUT_MS = 5000


class MongoDB:
    _client = None
    _db = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            try:
                cls._client = MongoClient(
                    MongoDBConfig.MONGODB_URI,
                    maxPoolSize=MongoDBConfig.MAX_POOL_SIZE,
                    minPoolSize=MongoDBConfig.MIN_POOL_SIZE,
                    connectTimeoutMS=MongoDBConfig.CONNECT_TIMEOUT_MS,
                    serverSelectionTimeoutMS=MongoDBConfig.SERVER_SELECTION_TIMEOUT_MS,
                    tls=True,
                    tlsAllowInvalidCertificates=True
                )
                cls._client.server_info()
                logger.info("Conectado ao MongoDB com sucesso")
            except ConnectionFailure as e:
                logger.error(f"Erro ao conectar ao MongoDB: {e}")
                raise
        return cls._client

    @classmethod
    def get_db(cls):
        if cls._db is None:
            client = cls.get_client()
            cls._db = client[MongoDBConfig.MONGODB_DB]
        return cls._db

    @classmethod
    def close_connection(cls):
        if cls._client is not None:
            cls._client.close()
            cls._client = None
            cls._db = None
            logger.info("Conexao com MongoDB encerrada")
