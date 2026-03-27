import os
import time
from google.ads.admanager_v1.services.network_service import NetworkServiceClient
from config.settings import Config
from client.redis import redis

TOKEN_EXPIRY_KEY = "dom_access_token_expiry"
LAST_REFRESH_KEY = "dom_access_token_last_refresh"


def get_authenticated_network_service():
    """Retorna um NetworkServiceClient autenticado"""
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.json_key_path

    now = int(time.time())

    # Checa se o ultimo refresh foi ha menos de 3500s (~58min)
    expiry = redis.get(TOKEN_EXPIRY_KEY)
    if expiry and now < int(expiry) - 60:
        return NetworkServiceClient()

    # Se expirou ou nao existe, forca novo client
    client = NetworkServiceClient()

    # Marca nova expiracao (tokens geralmente valem por 3600s)
    redis.set(TOKEN_EXPIRY_KEY, now + 3600)
    redis.set(LAST_REFRESH_KEY, now)

    return client
