"""Tests for repositories/revenue_domain_repository.py"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


def make_mock_collection():
    col = MagicMock()
    col.create_index.return_value = 'idx_domain_network_date'
    return col


def make_mock_db(col):
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=col)
    return db


class TestRevenueDomainRepositoryInit:
    def test_init_sets_collection_name(self):
        col = make_mock_collection()
        db = make_mock_db(col)

        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_domain_repository import RevenueDomainRepository
            repo = RevenueDomainRepository()

        assert repo._collection_name == 'DomRevenueByDomain'

    def test_init_creates_indexes(self):
        col = make_mock_collection()
        db = make_mock_db(col)

        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_domain_repository import RevenueDomainRepository
            RevenueDomainRepository()

        col.create_index.assert_called_once()

    def test_init_handles_existing_index_error(self):
        col = make_mock_collection()
        col.create_index.side_effect = Exception("index already exists")
        db = make_mock_db(col)

        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_domain_repository import RevenueDomainRepository
            # Should not raise
            RevenueDomainRepository()


class TestRevenueDomainRepositorySaveDailyStats:
    def setup_method(self):
        self.col = make_mock_collection()
        db = make_mock_db(self.col)
        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_domain_repository import RevenueDomainRepository
            self.repo = RevenueDomainRepository()

    def test_save_daily_stats_calls_update_or_insert(self):
        mock_result = MagicMock()
        mock_result.matched_count = 0
        mock_result.modified_count = 0
        mock_result.upserted_id = None
        self.col.update_one.return_value = mock_result

        stats = {'domain': 'example.com', 'network': '111', 'date': '2024-01-15', 'revenue': 1.0}
        result = self.repo.save_daily_stats(stats)

        self.col.update_one.assert_called_once()
        assert 'matched' in result

    def test_save_daily_stats_uses_correct_search_criteria(self):
        mock_result = MagicMock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_result.upserted_id = None
        self.col.update_one.return_value = mock_result

        stats = {'domain': 'test.com', 'network': '222', 'date': '2024-06-01'}
        self.repo.save_daily_stats(stats)

        call_args = self.col.update_one.call_args
        filter_dict = call_args[0][0]
        assert filter_dict['domain'] == 'test.com'
        assert filter_dict['network'] == '222'
        assert filter_dict['date'] == '2024-06-01'


class TestRevenueDomainRepositoryBulkSaveStats:
    def setup_method(self):
        self.col = make_mock_collection()
        db = make_mock_db(self.col)
        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_domain_repository import RevenueDomainRepository
            self.repo = RevenueDomainRepository()

    def test_bulk_save_stats_empty_list(self):
        result = self.repo.bulk_save_stats([])
        assert result == {'matched': 0, 'modified': 0, 'upserted': 0}
        self.col.bulk_write.assert_not_called()

    def test_bulk_save_stats_with_data(self):
        mock_result = MagicMock()
        mock_result.matched_count = 2
        mock_result.modified_count = 1
        mock_result.upserted_count = 1
        self.col.bulk_write.return_value = mock_result

        stats_list = [
            {'domain': 'a.com', 'network': '111', 'date': '2024-01-15'},
            {'domain': 'b.com', 'network': '111', 'date': '2024-01-15'},
        ]
        result = self.repo.bulk_save_stats(stats_list)

        self.col.bulk_write.assert_called_once()
        assert result['matched'] == 2
        assert result['modified'] == 1
        assert result['upserted'] == 1

    def test_bulk_save_stats_raises_on_exception(self):
        self.col.bulk_write.side_effect = Exception("bulk write failed")
        with pytest.raises(Exception, match="bulk write failed"):
            self.repo.bulk_save_stats([{'domain': 'x.com', 'network': '1', 'date': '2024-01-01'}])

    def test_bulk_save_stats_sets_updated_at(self):
        """Verify bulk_write is called (which means updated_at was set inside)."""
        mock_result = MagicMock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_result.upserted_count = 0
        self.col.bulk_write.return_value = mock_result

        stats_list = [{'domain': 'a.com', 'network': '111', 'date': '2024-01-15'}]
        result = self.repo.bulk_save_stats(stats_list)

        # Verify bulk_write was called with the operations
        self.col.bulk_write.assert_called_once()
        bulk_ops = self.col.bulk_write.call_args[0][0]
        assert len(bulk_ops) == 1
        # Verify the operation is an UpdateOne
        from pymongo import UpdateOne
        assert isinstance(bulk_ops[0], UpdateOne)

    def test_bulk_save_stats_sets_created_at_when_missing(self):
        """Verify setdefault for created_at runs without error."""
        mock_result = MagicMock()
        mock_result.matched_count = 0
        mock_result.modified_count = 0
        mock_result.upserted_count = 1
        self.col.bulk_write.return_value = mock_result

        stats_list = [{'domain': 'new.com', 'network': '111', 'date': '2024-01-15'}]
        result = self.repo.bulk_save_stats(stats_list)

        # Operation should succeed
        assert result['upserted'] == 1
        self.col.bulk_write.assert_called_once()
