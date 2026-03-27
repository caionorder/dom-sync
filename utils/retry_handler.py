import time
import random
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def exponential_backoff_retry(max_retries=5, initial_delay=1.0, max_delay=60.0, jitter=0.1):
    """
    Decorator para retry com backoff exponencial.
    Trata especificamente erros 429 (rate limit).
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if "429" in str(e) or "Resource has been exhausted" in str(e):
                        if attempt == max_retries - 1:
                            raise

                        # Adicionar jitter para evitar "thundering herd"
                        jittered_delay = delay * (1 + jitter * (random.random() * 2 - 1))

                        logger.warning(
                            f"Rate limit (429). Tentativa {attempt + 1}/{max_retries}. "
                            f"Aguardando {jittered_delay:.2f} segundos..."
                        )

                        time.sleep(jittered_delay)

                        # Aumentar o delay exponencialmente
                        delay = min(delay * 2, max_delay)
                    else:
                        raise
        return wrapper
    return decorator
