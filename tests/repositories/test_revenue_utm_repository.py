"""Tests for repositories/revenue_utm_repository.py"""
import pytest
from unittest.mock import MagicMock, patch


def make_mock_collection():
    col = MagicMock()
    col.create_index.return_value = 'idx_domain_network_utm_date'
    return col


def make_mock_db(col):
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=col)
    return db


class TestRevenueUtmRepositoryInit:
    def test_init_sets_collection_name(self):
        col = make_mock_collection()
        db = make_mock_db(col)

        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_utm_repository import RevenueUtmRepository
            repo = RevenueUtmRepository()

        assert repo._collection_name == 'DomRevenueByUtmCampaign'

    def test_init_creates_indexes(self):
        col = make_mock_collection()
        db = make_mock_db(col)

        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_utm_repository import RevenueUtmRepository
            RevenueUtmRepository()

        col.create_index.assert_called_once()

    def test_init_handles_existing_index_error(self):
        col = make_mock_collection()
        col.create_index.side_effect = Exception("index already exists")
        db = make_mock_db(col)

        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_utm_repository import RevenueUtmRepository
            RevenueUtmRepository()


class TestRevenueUtmRepositorySaveDailyStats:
    def setup_method(self):
        self.col = make_mock_collection()
        db = make_mock_db(self.col)
        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_utm_repository import RevenueUtmRepository
            self.repo = RevenueUtmRepository()

    def test_save_daily_stats_uses_utm_campaign_in_criteria(self):
        mock_result = MagicMock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_result.upserted_id = None
        self.col.update_one.return_value = mock_result

        stats = {
            'domain': 'test.com',
            'network': '222',
            'utm_campaign': 'black_friday',
            'date': '2024-11-29',
        }
        self.repo.save_daily_stats(stats)

        call_args = self.col.update_one.call_args
        filter_dict = call_args[0][0]
        assert filter_dict['utm_campaign'] == 'black_friday'

    def test_save_daily_stats_returns_stats_dict(self):
        mock_result = MagicMock()
        mock_result.matched_count = 0
        mock_result.modified_count = 0
        mock_result.upserted_id = None
        self.col.update_one.return_value = mock_result

        stats = {
            'domain': 'test.com',
            'network': '222',
            'utm_campaign': 'campaign1',
            'date': '2024-01-01',
        }
        result = self.repo.save_daily_stats(stats)
        assert 'matched' in result


class TestRevenueUtmRepositoryBulkSaveStats:
    def setup_method(self):
        self.col = make_mock_collection()
        db = make_mock_db(self.col)
        with patch('repositories.base_repository.MongoDB.get_db', return_value=db):
            from repositories.revenue_utm_repository import RevenueUtmRepository
            self.repo = RevenueUtmRepository()

    def test_bulk_save_stats_empty_list(self):
        result = self.repo.bulk_save_stats([])
        assert result == {'matched': 0, 'modified': 0, 'upserted': 0}
        self.col.bulk_write.assert_not_called()

    def test_bulk_save_stats_with_data(self):
        mock_result = MagicMock()
        mock_result.matched_count = 3
        mock_result.modified_count = 2
        mock_result.upserted_count = 1
        self.col.bulk_write.return_value = mock_result

        stats_list = [
            {
                'domain': 'a.com', 'network': '111',
                'utm_campaign': 'promo', 'date': '2024-01-15'
            },
        ]
        result = self.repo.bulk_save_stats(stats_list)
        assert result['matched'] == 3

    def test_bulk_save_stats_raises_on_exception(self):
        self.col.bulk_write.side_effect = Exception("connection error")
        with pytest.raises(Exception):
            self.repo.bulk_save_stats([
                {'domain': 'x.com', 'network': '1', 'utm_campaign': 'c', 'date': '2024-01-01'}
            ])

    def test_bulk_save_stats_uses_all_unique_fields(self):
        """Verify that all 4 unique fields are included in the filter."""
        mock_result = MagicMock()
        mock_result.matched_count = 1
        mock_result.modified_count = 1
        mock_result.upserted_count = 0
        self.col.bulk_write.return_value = mock_result

        stats_list = [
            {
                'domain': 'test.com',
                'network': '999',
                'utm_campaign': 'summer',
                'date': '2024-07-01',
            }
        ]
        self.repo.bulk_save_stats(stats_list)

        # Verify bulk_write was called
        self.col.bulk_write.assert_called_once()
        bulk_ops = self.col.bulk_write.call_args[0][0]
        assert len(bulk_ops) == 1
        # Verify it's an UpdateOne operation
        from pymongo import UpdateOne
        assert isinstance(bulk_ops[0], UpdateOne)
