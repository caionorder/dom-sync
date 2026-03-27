from typing import List, Dict, Any
from repositories.revenue_domain_repository import RevenueDomainRepository
from repositories.revenue_utm_repository import RevenueUtmRepository
import logging

logger = logging.getLogger(__name__)


class MetricsProcessor:
    """Processa metricas e salva nos repositorios apropriados usando bulk operations"""

    CHUNK_SIZE = 200

    def __init__(self):
        self.domain_repository = RevenueDomainRepository()
        self.utm_repository = RevenueUtmRepository()

    def process_domain_bulk(self, data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Processamento otimizado usando bulk operations para revenue by domain.

        Returns:
            Dict com estatisticas de processamento
        """
        stats = {
            'total': len(data),
            'processed': 0,
            'errors': 0
        }

        if not data:
            return stats

        for i in range(0, len(data), self.CHUNK_SIZE):
            chunk = data[i:i + self.CHUNK_SIZE]
            try:
                result = self.domain_repository.bulk_save_stats(chunk)
                stats['processed'] += sum(result.values())
                logger.info(f"Domain chunk processado: {i + len(chunk)}/{len(data)}")
            except Exception as e:
                logger.error(f"Erro ao processar domain chunk: {e}")
                stats['errors'] += len(chunk)

        logger.info(f"Processamento domain bulk concluido: {stats}")
        return stats

    def process_utm_bulk(self, data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Processamento otimizado usando bulk operations para revenue by utm_campaign.

        Returns:
            Dict com estatisticas de processamento
        """
        stats = {
            'total': len(data),
            'processed': 0,
            'errors': 0
        }

        if not data:
            return stats

        for i in range(0, len(data), self.CHUNK_SIZE):
            chunk = data[i:i + self.CHUNK_SIZE]
            try:
                result = self.utm_repository.bulk_save_stats(chunk)
                stats['processed'] += sum(result.values())
                logger.info(f"UTM chunk processado: {i + len(chunk)}/{len(data)}")
            except Exception as e:
                logger.error(f"Erro ao processar utm chunk: {e}")
                stats['errors'] += len(chunk)

        logger.info(f"Processamento utm bulk concluido: {stats}")
        return stats
