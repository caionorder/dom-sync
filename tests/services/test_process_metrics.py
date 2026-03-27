"""Tests for services/process_metrics.py"""
import pytest
from unittest.mock import MagicMock, patch


def make_mock_repo():
    mock = MagicMock()
    mock.bulk_save_stats.return_value = {'matched': 1, 'modified': 1, 'upserted': 0}
    return mock


class TestMetricsProcessorInit:
    def test_init_creates_repositories(self):
        mock_domain_repo = make_mock_repo()
        mock_utm_repo = make_mock_repo()

        with patch('services.process_metrics.RevenueDomainRepository', return_value=mock_domain_repo), \
             patch('services.process_metrics.RevenueUtmRepository', return_value=mock_utm_repo):
            from services.process_metrics import MetricsProcessor
            processor = MetricsProcessor()

        assert processor.domain_repository is mock_domain_repo
        assert processor.utm_repository is mock_utm_repo


class TestMetricsProcessorProcessDomainBulk:
    def setup_method(self):
        self.mock_domain_repo = make_mock_repo()
        self.mock_utm_repo = make_mock_repo()

        with patch('services.process_metrics.RevenueDomainRepository', return_value=self.mock_domain_repo), \
             patch('services.process_metrics.RevenueUtmRepository', return_value=self.mock_utm_repo):
            from services.process_metrics import MetricsProcessor
            self.processor = MetricsProcessor()

    def test_empty_data_returns_zero_stats(self):
        result = self.processor.process_domain_bulk([])
        assert result == {'total': 0, 'processed': 0, 'errors': 0}
        self.mock_domain_repo.bulk_save_stats.assert_not_called()

    def test_processes_single_chunk(self):
        data = [{'domain': 'a.com', 'date': '2024-01-15'}] * 5
        self.mock_domain_repo.bulk_save_stats.return_value = {'matched': 5, 'modified': 3, 'upserted': 2}

        result = self.processor.process_domain_bulk(data)

        assert result['total'] == 5
        assert result['processed'] == 10  # sum of matched+modified+upserted = 5+3+2
        assert result['errors'] == 0
        self.mock_domain_repo.bulk_save_stats.assert_called_once()

    def test_processes_multiple_chunks(self):
        # CHUNK_SIZE = 200; create 250 items to force 2 chunks
        data = [{'domain': f'domain{i}.com', 'date': '2024-01-15'} for i in range(250)]
        self.mock_domain_repo.bulk_save_stats.return_value = {'matched': 1, 'modified': 1, 'upserted': 0}

        result = self.processor.process_domain_bulk(data)

        assert self.mock_domain_repo.bulk_save_stats.call_count == 2
        assert result['total'] == 250

    def test_errors_counted_on_exception(self):
        data = [{'domain': 'a.com'}, {'domain': 'b.com'}]
        self.mock_domain_repo.bulk_save_stats.side_effect = Exception("DB error")

        result = self.processor.process_domain_bulk(data)

        assert result['errors'] == 2
        assert result['processed'] == 0

    def test_partial_chunk_error_counted(self):
        # 201 items = chunk of 200 + chunk of 1
        data = [{'domain': f'd{i}.com'} for i in range(201)]
        call_results = [
            {'matched': 1, 'modified': 1, 'upserted': 0},  # first chunk succeeds
        ]
        self.mock_domain_repo.bulk_save_stats.side_effect = [
            call_results[0],
            Exception("second chunk failed"),
        ]

        result = self.processor.process_domain_bulk(data)

        assert result['errors'] == 1  # last chunk of 1 item failed
        assert result['processed'] == 2  # sum of first chunk stats


class TestMetricsProcessorProcessUtmBulk:
    def setup_method(self):
        self.mock_domain_repo = make_mock_repo()
        self.mock_utm_repo = make_mock_repo()

        with patch('services.process_metrics.RevenueDomainRepository', return_value=self.mock_domain_repo), \
             patch('services.process_metrics.RevenueUtmRepository', return_value=self.mock_utm_repo):
            from services.process_metrics import MetricsProcessor
            self.processor = MetricsProcessor()

    def test_empty_data_returns_zero_stats(self):
        result = self.processor.process_utm_bulk([])
        assert result == {'total': 0, 'processed': 0, 'errors': 0}
        self.mock_utm_repo.bulk_save_stats.assert_not_called()

    def test_processes_utm_data(self):
        data = [{'domain': 'a.com', 'utm_campaign': 'promo'}] * 3
        self.mock_utm_repo.bulk_save_stats.return_value = {'matched': 3, 'modified': 3, 'upserted': 0}

        result = self.processor.process_utm_bulk(data)

        assert result['total'] == 3
        assert result['errors'] == 0
        self.mock_utm_repo.bulk_save_stats.assert_called_once()

    def test_errors_counted_on_exception(self):
        data = [{'domain': 'a.com', 'utm_campaign': 'p'}]
        self.mock_utm_repo.bulk_save_stats.side_effect = Exception("write error")

        result = self.processor.process_utm_bulk(data)

        assert result['errors'] == 1

    def test_multiple_chunks_for_utm(self):
        data = [{'domain': f'd{i}.com', 'utm_campaign': 'c'} for i in range(205)]
        self.mock_utm_repo.bulk_save_stats.return_value = {'matched': 1, 'modified': 1, 'upserted': 0}

        self.processor.process_utm_bulk(data)

        assert self.mock_utm_repo.bulk_save_stats.call_count == 2
