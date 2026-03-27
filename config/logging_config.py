"""
Configuracao de logging para suprimir mensagens desnecessarias
"""
import logging


def configure_logging():
    """Configura o logging para suprimir mensagens de erro do googleads"""

    # Desabilita logs da biblioteca googleads
    logging.getLogger('googleads').setLevel(logging.CRITICAL)
    logging.getLogger('googleads.common').setLevel(logging.CRITICAL)
    logging.getLogger('googleads.errors').setLevel(logging.CRITICAL)
    logging.getLogger('googleads.soap').setLevel(logging.CRITICAL)

    # Desabilita logs do zeep (usado pelo googleads)
    logging.getLogger('zeep').setLevel(logging.CRITICAL)
    logging.getLogger('zeep.transports').setLevel(logging.CRITICAL)

    # Desabilita logs do urllib3
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    # Desabilita logs do requests
    logging.getLogger('requests').setLevel(logging.CRITICAL)

    # Mantem apenas logs criticos para o logger principal
    logging.getLogger().setLevel(logging.WARNING)


# Configura automaticamente ao importar
configure_logging()
