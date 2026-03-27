import logging
import threading


def setup_logging(debug=False):
    """Configura logging para multiprocessamento"""

    class ThreadFormatter(logging.Formatter):
        """Formatter que inclui thread ID"""
        def format(self, record):
            record.thread_id = threading.current_thread().ident
            return super().format(record)

    level = logging.DEBUG if debug else logging.INFO

    formatter = ThreadFormatter(
        '%(asctime)s - Thread-%(thread_id)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger()
    logger.setLevel(level)

    # Remover handlers existentes
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # FileHandler ao inves de StreamHandler
    file_handler = logging.FileHandler('dom_processing.log', mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Silenciar loggers barulhentos
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)

    return logger
