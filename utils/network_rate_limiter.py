import time
import threading
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class NetworkRateLimiter:
    """Rate limiter que mantem limite separado para cada network"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton pattern para garantir uma unica instancia"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, requests_per_second=2):
        if hasattr(self, '_initialized'):
            return

        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second

        # Estrutura para rastrear requisicoes por segundo
        self.request_windows = defaultdict(list)  # network_id -> [timestamps]
        self.locks = defaultdict(threading.Lock)
        self.global_lock = threading.Lock()
        self._initialized = True

    def _get_lock(self, network_id: str):
        """Obtem ou cria um lock para a network"""
        with self.global_lock:
            return self.locks[network_id]

    def wait_if_needed(self, network_id: str):
        """Espera se necessario para respeitar o rate limit da network especifica"""
        lock = self._get_lock(network_id)

        with lock:
            current_time = time.time()

            # Remove timestamps antigos (mais de 1 segundo)
            self.request_windows[network_id] = [
                ts for ts in self.request_windows[network_id]
                if current_time - ts < 1.0
            ]

            # Se ja fez N requisicoes no ultimo segundo
            if len(self.request_windows[network_id]) >= self.requests_per_second:
                oldest_request = self.request_windows[network_id][0]
                wait_time = 1.0 - (current_time - oldest_request)
                logger.debug(
                    f"[{network_id}] Esperando {wait_time:.3f}s para respeitar "
                    f"o limite de {self.requests_per_second} req/s"
                )

                if wait_time > 0:
                    time.sleep(wait_time)

                    # Limpa timestamps novamente apos espera
                    current_time = time.time()
                    self.request_windows[network_id] = [
                        ts for ts in self.request_windows[network_id]
                        if current_time - ts < 1.0
                    ]

            # Adiciona timestamp atual
            self.request_windows[network_id].append(current_time)

            logger.debug(
                f"[{network_id}] Requisicao permitida. "
                f"Total no ultimo segundo: {len(self.request_windows[network_id])}"
            )


# Instancia singleton
network_rate_limiter = NetworkRateLimiter(requests_per_second=2)
