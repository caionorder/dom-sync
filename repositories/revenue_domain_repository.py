from repositories.base_repository import BaseRepository
from datetime import datetime
from pymongo import UpdateOne, ASCENDING
import logging

logger = logging.getLogger(__name__)


class RevenueDomainRepository(BaseRepository):
    """Repositorio para colecao DomRevenueByDomain"""

    UNIQUE_FIELDS = ['domain', 'network', 'date']

    def __init__(self):
        super().__init__('DomRevenueByDomain')
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
        logger.info(f"bulk_save_stats: received {len(stats_list)} records for collection '{self._collection_name}'")
        try:
            bulk_operations = []

            for i, data in enumerate(stats_list):
                filter_dict = {field: data[field] for field in self.UNIQUE_FIELDS if field in data}

                # Log the first record's filter to verify keys are present
                if i == 0:
                    logger.info(f"bulk_save_stats: SAMPLE filter_dict: {filter_dict}")
                    missing_fields = [f for f in self.UNIQUE_FIELDS if f not in data]
                    if missing_fields:
                        logger.warning(f"bulk_save_stats: MISSING unique fields in data: {missing_fields}")

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

            logger.info(f"bulk_save_stats: executing {len(bulk_operations)} bulk operations")

            if bulk_operations:
                result = self.collection.bulk_write(bulk_operations)
                result_dict = {
                    'matched': result.matched_count,
                    'modified': result.modified_count,
                    'upserted': result.upserted_count
                }
                logger.info(f"bulk_save_stats: BulkWriteResult: {result_dict}")
                return result_dict

            return {'matched': 0, 'modified': 0, 'upserted': 0}

        except Exception as e:
            logger.error(f"Erro em bulk_save_stats: {e}", exc_info=True)
            raise
