from repositories.base_repository import BaseRepository
from datetime import datetime
from pymongo import UpdateOne, ASCENDING
import logging

logger = logging.getLogger(__name__)


class RevenueDomainRepository(BaseRepository):
    """Repositorio para colecao dom_revenue_by_domain"""

    UNIQUE_FIELDS = ['domain', 'network', 'date']

    def __init__(self):
        super().__init__('dom_revenue_by_domain')
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Cria indices unicos para a colecao"""
        try:
            self.collection.create_index(
                [
                    ('domain', ASCENDING),
                    ('network', ASCENDING),
                    ('date', ASCENDING),
                ],
                unique=True,
                name='idx_domain_network_date'
            )
        except Exception as e:
            logger.debug(f"Indice ja existe ou erro ao criar: {e}")

    def save_daily_stats(self, stats_data):
        """Salva estatisticas diarias usando updateOrCreate"""
        search_criteria = {
            'domain': stats_data.get('domain'),
            'network': stats_data.get('network'),
            'date': stats_data.get('date'),
        }
        return self.update_or_insert(search_criteria, stats_data)

    def bulk_save_stats(self, stats_list):
        """Salva multiplas estatisticas em bulk"""
        try:
            bulk_operations = []

            for data in stats_list:
                filter_dict = {field: data[field] for field in self.UNIQUE_FIELDS if field in data}

                update_data = data.copy()
                update_data['updated_at'] = datetime.utcnow()
                update_data.setdefault('created_at', datetime.utcnow())

                bulk_operations.append(
                    UpdateOne(
                        filter_dict,
                        {'$set': update_data},
                        upsert=True
                    )
                )

            if bulk_operations:
                result = self.collection.bulk_write(bulk_operations)
                return {
                    'matched': result.matched_count,
                    'modified': result.modified_count,
                    'upserted': result.upserted_count
                }

            return {'matched': 0, 'modified': 0, 'upserted': 0}

        except Exception as e:
            logger.error(f"Erro em bulk_save_stats: {e}")
            raise
