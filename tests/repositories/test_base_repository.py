"""Tests for repositories/base_repository.py"""
import pytest
from unittest.mock import MagicMock, patch, call
from bson import ObjectId


def make_collection():
    return MagicMock()


class TestQueryBuilderWhere:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.col = make_collection()
        self.qb = QueryBuilder(self.col)

    def test_where_eq_operator(self):
        result = self.qb.where('field', 'value')
        assert self.qb.filter_dict == {'field': 'value'}
        assert result is self.qb

    def test_where_custom_operator(self):
        self.qb.where('age', 30, operator='$gt')
        assert self.qb.filter_dict == {'age': {'$gt': 30}}

    def test_where_multiple_conditions_same_field(self):
        self.qb.where('age', 10, '$gte').where('age', 20, '$lte')
        assert self.qb.filter_dict['age'] == {'$gte': 10, '$lte': 20}


class TestQueryBuilderWhereIn:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.qb = QueryBuilder(make_collection())

    def test_where_in(self):
        self.qb.where_in('status', ['active', 'pending'])
        assert self.qb.filter_dict == {'status': {'$in': ['active', 'pending']}}
        assert self.qb.where_in('status', ['x']) is self.qb


class TestQueryBuilderWhereBetween:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.qb = QueryBuilder(make_collection())

    def test_where_between(self):
        self.qb.where_between('date', '2024-01-01', '2024-01-31')
        assert self.qb.filter_dict == {'date': {'$gte': '2024-01-01', '$lte': '2024-01-31'}}


class TestQueryBuilderWhereLike:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.qb = QueryBuilder(make_collection())

    def test_where_like(self):
        self.qb.where_like('name', 'example')
        assert self.qb.filter_dict == {'name': {'$regex': 'example', '$options': 'i'}}


class TestQueryBuilderOrderBy:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.qb = QueryBuilder(make_collection())

    def test_order_by_asc(self):
        self.qb.order_by('name', 'asc')
        assert self.qb.sort_fields == [('name', 1)]

    def test_order_by_desc(self):
        self.qb.order_by('name', 'desc')
        assert self.qb.sort_fields == [('name', -1)]

    def test_order_by_default_is_asc(self):
        self.qb.order_by('name')
        assert self.qb.sort_fields == [('name', 1)]


class TestQueryBuilderSkipLimit:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.qb = QueryBuilder(make_collection())

    def test_skip(self):
        result = self.qb.skip(10)
        assert self.qb.skip_value == 10
        assert result is self.qb

    def test_limit(self):
        result = self.qb.limit(20)
        assert self.qb.limit_value == 20
        assert result is self.qb


class TestQueryBuilderSelect:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.qb = QueryBuilder(make_collection())

    def test_select_adds_fields(self):
        self.qb.select('name', 'date')
        assert self.qb.projection_fields == {'name': 1, 'date': 1}

    def test_select_appends_to_existing(self):
        self.qb.select('name').select('date')
        assert self.qb.projection_fields == {'name': 1, 'date': 1}


class TestQueryBuilderConvertIds:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.qb = QueryBuilder(make_collection())

    def test_convert_ids_list(self):
        oid = ObjectId()
        docs = [{'_id': oid, 'name': 'test'}]
        result = self.qb._convert_ids(docs)
        assert result[0]['_id'] == str(oid)

    def test_convert_ids_single_doc(self):
        oid = ObjectId()
        doc = {'_id': oid, 'name': 'test'}
        result = self.qb._convert_ids(doc)
        assert result['_id'] == str(oid)

    def test_convert_ids_no_id_field(self):
        docs = [{'name': 'test'}]
        result = self.qb._convert_ids(docs)
        assert result == [{'name': 'test'}]

    def test_convert_ids_none_returns_none(self):
        result = self.qb._convert_ids(None)
        assert result is None


class TestQueryBuilderGet:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.col = make_collection()
        self.qb = QueryBuilder(self.col)

    def test_get_calls_find(self):
        oid = ObjectId()
        self.col.find.return_value = [{'_id': oid, 'name': 'doc'}]
        result = self.qb.get()
        self.col.find.assert_called_once()
        assert result[0]['name'] == 'doc'

    def test_get_with_sort(self):
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        self.col.find.return_value = mock_cursor

        self.qb.order_by('name', 'asc')
        result = self.qb.get()
        mock_cursor.sort.assert_called_once_with([('name', 1)])

    def test_get_with_skip(self):
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        self.col.find.return_value = mock_cursor

        self.qb.skip(5)
        # Force list() — mock __iter__
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        self.qb.get()
        mock_cursor.skip.assert_called_once_with(5)

    def test_get_with_limit(self):
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        self.col.find.return_value = mock_cursor

        self.qb.limit(10)
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        self.qb.get()
        mock_cursor.limit.assert_called_once_with(10)

    def test_get_raises_on_exception(self):
        self.col.find.side_effect = Exception("DB error")
        with pytest.raises(Exception, match="DB error"):
            self.qb.get()


class TestQueryBuilderFirst:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.col = make_collection()
        self.qb = QueryBuilder(self.col)

    def _make_cursor(self, docs):
        cursor = MagicMock()
        cursor.sort.return_value = cursor
        cursor.skip.return_value = cursor
        cursor.limit.return_value = cursor
        cursor.__iter__ = MagicMock(return_value=iter(docs))
        # list(cursor) — support via spec
        list_mock = MagicMock(return_value=docs)
        # patch list() call — actually MagicMock supports iter by default
        cursor.__iter__ = MagicMock(return_value=iter(docs))
        return cursor

    def test_first_returns_first_element(self):
        oid = ObjectId()
        docs = [{'_id': oid, 'name': 'first'}]
        cursor = self._make_cursor(docs)
        self.col.find.return_value = cursor
        result = self.qb.first()
        assert result['name'] == 'first'

    def test_first_returns_none_when_empty(self):
        cursor = self._make_cursor([])
        self.col.find.return_value = cursor
        result = self.qb.first()
        assert result is None


class TestQueryBuilderCount:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.col = make_collection()
        self.qb = QueryBuilder(self.col)

    def test_count_calls_count_documents(self):
        self.col.count_documents.return_value = 5
        result = self.qb.count()
        assert result == 5

    def test_count_raises_on_exception(self):
        self.col.count_documents.side_effect = Exception("error")
        with pytest.raises(Exception):
            self.qb.count()


class TestQueryBuilderExists:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.col = make_collection()
        self.qb = QueryBuilder(self.col)

    def test_exists_returns_true_when_count_positive(self):
        self.col.count_documents.return_value = 1
        assert self.qb.exists() is True

    def test_exists_returns_false_when_count_zero(self):
        self.col.count_documents.return_value = 0
        assert self.qb.exists() is False


class TestQueryBuilderAggregate:
    def setup_method(self):
        from repositories.base_repository import QueryBuilder
        self.col = make_collection()
        self.qb = QueryBuilder(self.col)

    def test_aggregate_without_filter(self):
        self.col.aggregate.return_value = [{'count': 5}]
        result = self.qb.aggregate([{'$group': {'_id': None, 'count': {'$sum': 1}}}])
        self.col.aggregate.assert_called_once()
        assert result == [{'count': 5}]

    def test_aggregate_prepends_match_when_filter_set(self):
        self.qb.where('status', 'active')
        self.col.aggregate.return_value = []
        pipeline = [{'$group': {'_id': '$name'}}]
        self.qb.aggregate(pipeline)
        called_pipeline = self.col.aggregate.call_args[0][0]
        assert called_pipeline[0] == {'$match': {'status': 'active'}}

    def test_aggregate_raises_on_exception(self):
        self.col.aggregate.side_effect = Exception("agg error")
        with pytest.raises(Exception):
            self.qb.aggregate([])


class TestBaseRepositoryMethods:
    def setup_method(self):
        self.mock_db = MagicMock()
        self.mock_collection = MagicMock()
        self.mock_db.__getitem__ = MagicMock(return_value=self.mock_collection)

        with patch('repositories.base_repository.MongoDB.get_db', return_value=self.mock_db):
            from repositories.base_repository import BaseRepository
            self.repo = BaseRepository('test_collection')

    def test_query_returns_query_builder(self):
        from repositories.base_repository import QueryBuilder
        qb = self.repo.query()
        assert isinstance(qb, QueryBuilder)

    def test_all_calls_get(self):
        self.mock_collection.find.return_value = []
        result = self.repo.all()
        assert result == []

    def _make_cursor(self, docs):
        cursor = MagicMock()
        cursor.sort.return_value = cursor
        cursor.skip.return_value = cursor
        cursor.limit.return_value = cursor
        cursor.__iter__ = MagicMock(return_value=iter(docs))
        return cursor

    def test_find_by_string_id(self):
        oid = ObjectId()
        docs = [{'_id': oid, 'name': 'doc'}]
        self.mock_collection.find.return_value = self._make_cursor(docs)
        result = self.repo.find(str(oid))
        assert result['name'] == 'doc'

    def test_find_by_object_id(self):
        oid = ObjectId()
        docs = [{'_id': oid}]
        self.mock_collection.find.return_value = self._make_cursor(docs)
        result = self.repo.find(oid)
        assert result is not None

    def test_where_returns_query_builder(self):
        from repositories.base_repository import QueryBuilder
        result = self.repo.where('field', 'value')
        assert isinstance(result, QueryBuilder)

    def test_order_by_returns_query_builder(self):
        from repositories.base_repository import QueryBuilder
        result = self.repo.order_by('name')
        assert isinstance(result, QueryBuilder)

    def test_create_inserts_document(self):
        inserted_id = ObjectId()
        mock_result = MagicMock()
        mock_result.inserted_id = inserted_id
        self.mock_collection.insert_one.return_value = mock_result

        data = {'name': 'test'}
        result = self.repo.create(data)

        self.mock_collection.insert_one.assert_called_once()
        assert result['_id'] == str(inserted_id)
        assert 'created_at' in result
        assert 'updated_at' in result

    def test_create_raises_on_exception(self):
        self.mock_collection.insert_one.side_effect = Exception("insert failed")
        with pytest.raises(Exception, match="insert failed"):
            self.repo.create({'name': 'test'})

    def test_update_by_string_id(self):
        oid = ObjectId()
        mock_result = MagicMock()
        mock_result.modified_count = 1
        self.mock_collection.update_one.return_value = mock_result

        result = self.repo.update(str(oid), {'name': 'updated'})
        assert result is True

    def test_update_by_object_id(self):
        oid = ObjectId()
        mock_result = MagicMock()
        mock_result.modified_count = 1
        self.mock_collection.update_one.return_value = mock_result

        result = self.repo.update(oid, {'name': 'updated'})
        assert result is True

    def test_update_returns_false_when_no_modification(self):
        oid = ObjectId()
        mock_result = MagicMock()
        mock_result.modified_count = 0
        self.mock_collection.update_one.return_value = mock_result

        result = self.repo.update(str(oid), {'name': 'same'})
        assert result is False

    def test_update_raises_on_exception(self):
        self.mock_collection.update_one.side_effect = Exception("update failed")
        with pytest.raises(Exception):
            self.repo.update(str(ObjectId()), {'name': 'x'})

    def test_update_or_insert_returns_stats(self):
        mock_result = MagicMock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_result.upserted_id = None
        self.mock_collection.update_one.return_value = mock_result

        result = self.repo.update_or_insert({'domain': 'x'}, {'revenue': 1.0})
        assert result['matched'] == 1
        assert result['modified'] == 1
        assert result['upserted_id'] is None

    def test_update_or_insert_with_upserted_id(self):
        mock_result = MagicMock()
        mock_result.matched_count = 0
        mock_result.modified_count = 0
        mock_result.upserted_id = ObjectId()
        self.mock_collection.update_one.return_value = mock_result

        result = self.repo.update_or_insert({'domain': 'new'}, {'revenue': 1.0})
        assert result['upserted_id'] is not None

    def test_update_or_insert_raises_on_exception(self):
        self.mock_collection.update_one.side_effect = Exception("upsert failed")
        with pytest.raises(Exception):
            self.repo.update_or_insert({'k': 'v'}, {'k': 'v'})

    def test_bulk_update_or_create_with_data(self):
        mock_result = MagicMock()
        mock_result.matched_count = 2
        mock_result.modified_count = 2
        mock_result.upserted_count = 0
        self.mock_collection.bulk_write.return_value = mock_result

        data = [
            {'domain': 'a.com', 'date': '2024-01-01'},
            {'domain': 'b.com', 'date': '2024-01-01'},
        ]
        result = self.repo.bulk_update_or_create(data, ['domain', 'date'])
        assert result['matched'] == 2
        assert result['modified'] == 2
        assert result['upserted'] == 0

    def test_bulk_update_or_create_empty_list(self):
        result = self.repo.bulk_update_or_create([], ['domain'])
        assert result == {'matched': 0, 'modified': 0, 'upserted': 0}
        self.mock_collection.bulk_write.assert_not_called()

    def test_bulk_update_or_create_raises_on_exception(self):
        self.mock_collection.bulk_write.side_effect = Exception("bulk failed")
        with pytest.raises(Exception):
            self.repo.bulk_update_or_create([{'domain': 'x'}], ['domain'])

    def test_truncate_deletes_all(self):
        mock_result = MagicMock()
        mock_result.deleted_count = 5
        self.mock_collection.delete_many.return_value = mock_result

        result = self.repo.truncate()
        self.mock_collection.delete_many.assert_called_once_with({})
        assert result is True

    def test_truncate_returns_false_when_nothing_deleted(self):
        mock_result = MagicMock()
        mock_result.deleted_count = 0
        self.mock_collection.delete_many.return_value = mock_result

        result = self.repo.truncate()
        assert result is False

    def test_truncate_raises_on_exception(self):
        self.mock_collection.delete_many.side_effect = Exception("truncate error")
        with pytest.raises(Exception):
            self.repo.truncate()
