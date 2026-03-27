from datetime import datetime
from bson import ObjectId
from config.mongodb import MongoDB
import logging

logger = logging.getLogger(__name__)


class QueryBuilder:
    def __init__(self, collection):
        self.collection = collection
        self.filter_dict = {}
        self.sort_fields = []
        self.skip_value = 0
        self.limit_value = 0
        self.projection_fields = None

    def where(self, field, value, operator='$eq'):
        """Adiciona uma condicao de filtro"""
        if operator == '$eq':
            self.filter_dict[field] = value
        else:
            if field not in self.filter_dict:
                self.filter_dict[field] = {}
            self.filter_dict[field][operator] = value
        return self

    def where_in(self, field, values):
        """Filtro para valores em uma lista"""
        self.filter_dict[field] = {'$in': values}
        return self

    def where_between(self, field, start, end):
        """Filtro para valores entre dois valores"""
        self.filter_dict[field] = {'$gte': start, '$lte': end}
        return self

    def where_like(self, field, pattern):
        """Filtro para padroes de texto (similar ao LIKE do SQL)"""
        self.filter_dict[field] = {'$regex': pattern, '$options': 'i'}
        return self

    def order_by(self, field, direction='asc'):
        """Adiciona ordenacao"""
        sort_direction = 1 if direction.lower() == 'asc' else -1
        self.sort_fields.append((field, sort_direction))
        return self

    def skip(self, value):
        """Define o skip para paginacao"""
        self.skip_value = value
        return self

    def limit(self, value):
        """Define o limite de documentos"""
        self.limit_value = value
        return self

    def select(self, *fields):
        """Define os campos a serem retornados"""
        if not self.projection_fields:
            self.projection_fields = {}
        for field in fields:
            self.projection_fields[field] = 1
        return self

    def _convert_ids(self, documents):
        """Converte _id de ObjectId para string em documentos"""
        if isinstance(documents, list):
            for doc in documents:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
        elif documents and '_id' in documents:
            documents['_id'] = str(documents['_id'])
        return documents

    def get(self):
        """Executa a query e retorna multiplos documentos"""
        try:
            cursor = self.collection.find(self.filter_dict, self.projection_fields)

            if self.sort_fields:
                cursor = cursor.sort(self.sort_fields)

            if self.skip_value > 0:
                cursor = cursor.skip(self.skip_value)

            if self.limit_value > 0:
                cursor = cursor.limit(self.limit_value)

            documents = list(cursor)
            return self._convert_ids(documents)
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            raise

    def first(self):
        """Retorna o primeiro documento"""
        self.limit_value = 1
        results = self.get()
        return results[0] if results else None

    def count(self):
        """Conta os documentos"""
        try:
            return self.collection.count_documents(self.filter_dict)
        except Exception as e:
            logger.error(f"Erro ao contar documentos: {e}")
            raise

    def exists(self):
        """Verifica se existe algum documento"""
        return self.count() > 0

    def aggregate(self, pipeline):
        """Executa um pipeline de agregacao"""
        try:
            if self.filter_dict:
                pipeline.insert(0, {'$match': self.filter_dict})

            results = list(self.collection.aggregate(pipeline))
            return self._convert_ids(results)
        except Exception as e:
            logger.error(f"Erro ao executar agregacao: {e}")
            raise


class BaseRepository:
    def __init__(self, collection_name):
        self.db = MongoDB.get_db()
        self.collection = self.db[collection_name]
        self._collection_name = collection_name

    def query(self):
        """Inicia um query builder"""
        return QueryBuilder(self.collection)

    def where(self, field, value, operator='$eq'):
        """Atalho para iniciar uma query com where"""
        return self.query().where(field, value, operator)

    def all(self):
        """Retorna todos os documentos"""
        return self.query().get()

    def find(self, id):
        """Busca por ID"""
        if isinstance(id, str):
            id = ObjectId(id)
        result = self.query().where('_id', id).first()
        return result

    def order_by(self, field, direction='asc'):
        """Atalho para iniciar uma query com order_by"""
        return self.query().order_by(field, direction)

    def _prepare_for_insert(self, data):
        """Prepara dados para insercao adicionando timestamps"""
        data['created_at'] = datetime.utcnow()
        data['updated_at'] = datetime.utcnow()
        return data

    def _prepare_for_update(self, data):
        """Prepara dados para atualizacao"""
        data['updated_at'] = datetime.utcnow()
        return data

    def create(self, data):
        """Cria um novo documento"""
        try:
            data = self._prepare_for_insert(data)
            result = self.collection.insert_one(data)
            data['_id'] = str(result.inserted_id)
            return data
        except Exception as e:
            logger.error(f"Erro ao criar documento: {e}")
            raise

    def update(self, id, data):
        """Atualiza um documento"""
        try:
            if isinstance(id, str):
                id = ObjectId(id)

            data = self._prepare_for_update(data)
            result = self.collection.update_one(
                {'_id': id},
                {'$set': data}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Erro ao atualizar documento: {e}")
            raise

    def update_or_insert(self, filter_dict, update_dict, upsert=True):
        """Atualiza ou insere usando operacao nativa do MongoDB"""
        try:
            update_data = self._prepare_for_update(update_dict.copy())

            result = self.collection.update_one(
                filter_dict,
                {'$set': update_data},
                upsert=upsert
            )

            return {
                'matched': result.matched_count,
                'modified': result.modified_count,
                'upserted_id': str(result.upserted_id) if result.upserted_id else None
            }

        except Exception as e:
            logger.error(f"Erro em updateOrInsert: {e}")
            raise

    def bulk_update_or_create(self, data_list, unique_fields):
        """
        Atualiza ou cria multiplos documentos em bulk.

        Args:
            data_list: Lista de documentos
            unique_fields: Lista de campos que identificam unicidade

        Returns:
            dict: Estatisticas da operacao
        """
        try:
            from pymongo import UpdateOne

            bulk_operations = []

            for data in data_list:
                filter_dict = {field: data[field] for field in unique_fields if field in data}

                update_data = self._prepare_for_update(data.copy())

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
            logger.error(f"Erro em bulkUpdateOrCreate: {e}")
            raise

    def truncate(self):
        """Remove todos os documentos da colecao"""
        try:
            result = self.collection.delete_many({})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Erro ao truncar colecao: {e}")
            raise
