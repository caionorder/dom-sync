"""Tests for services/dom_report_runner.py"""
import pytest
from unittest.mock import MagicMock, patch
from services.dom_report_runner import parse_day_parameter, run, ReportType


class TestParseDayParameter:
    def test_yesterday(self):
        result = parse_day_parameter('yesterday')
        assert result.start == 1
        assert result.end == 1

    def test_today(self):
        result = parse_day_parameter('today')
        assert result.start == 0
        assert result.end == 0

    def test_last_7_days(self):
        result = parse_day_parameter('last_7_days')
        assert result.start == 7
        assert result.end == 0

    def test_last_30_days(self):
        result = parse_day_parameter('last_30_days')
        assert result.start == 30
        assert result.end == 0

    def test_last_x_days_dynamic(self):
        result = parse_day_parameter('last_90_days')
        assert result.start == 90
        assert result.end == 0

    def test_last_1_day_dynamic(self):
        result = parse_day_parameter('last_1_days')
        assert result.start == 1
        assert result.end == 0

    def test_last_365_days_dynamic(self):
        result = parse_day_parameter('last_365_days')
        assert result.start == 365
        assert result.end == 0

    def test_invalid_day_returns_none(self):
        result = parse_day_parameter('invalid_period')
        assert result is None

    def test_empty_string_returns_none(self):
        result = parse_day_parameter('')
        assert result is None

    def test_partial_match_returns_none(self):
        result = parse_day_parameter('last_days')
        assert result is None


class TestReportType:
    def test_report_type_stores_start_end(self):
        rt = ReportType(start=5, end=2)
        assert rt.start == 5
        assert rt.end == 2


class TestRunFunction:
    def _make_mock_processor(self):
        mock = MagicMock()
        mock.process_domain_bulk.return_value = {'total': 1, 'processed': 1, 'errors': 0}
        mock.process_utm_bulk.return_value = {'total': 1, 'processed': 1, 'errors': 0}
        return mock

    def _make_mock_metrics_service(self):
        mock = MagicMock()
        mock.process_domain_metrics.return_value = [{'domain': 'example.com'}]
        mock.process_utm_campaign_metrics.return_value = [{'domain': 'example.com'}]
        return mock

    def test_run_returns_none_on_invalid_day(self):
        result = run('NET001', 'domain', 'bad_period')
        assert result is None

    def test_run_domain_report_success(self):
        mock_gam = MagicMock()
        mock_gam.gam_revenue_by_domain.return_value = [{'domain': 'test.com'}]
        mock_metrics = self._make_mock_metrics_service()
        mock_processor = self._make_mock_processor()

        with patch('services.dom_report_runner.GamService', return_value=mock_gam), \
             patch('services.dom_report_runner.MetricsReportService', return_value=mock_metrics), \
             patch('services.dom_report_runner.MetricsProcessor', return_value=mock_processor):
            result = run('NET001', 'domain', 'yesterday')

        assert result is not None
        mock_gam.gam_revenue_by_domain.assert_called_once()
        mock_metrics.process_domain_metrics.assert_called_once()
        mock_processor.process_domain_bulk.assert_called_once()

    def test_run_utm_campaign_report_success(self):
        mock_gam = MagicMock()
        mock_gam.gam_revenue_by_utm_campaign.return_value = [{'domain': 'test.com'}]
        mock_metrics = self._make_mock_metrics_service()
        mock_processor = self._make_mock_processor()

        with patch('services.dom_report_runner.GamService', return_value=mock_gam), \
             patch('services.dom_report_runner.MetricsReportService', return_value=mock_metrics), \
             patch('services.dom_report_runner.MetricsProcessor', return_value=mock_processor):
            result = run('NET001', 'utm_campaign', 'yesterday')

        mock_gam.gam_revenue_by_utm_campaign.assert_called_once()
        mock_metrics.process_utm_campaign_metrics.assert_called_once()
        mock_processor.process_utm_bulk.assert_called_once()

    def test_run_invalid_report_type_returns_none(self):
        mock_gam = MagicMock()

        with patch('services.dom_report_runner.GamService', return_value=mock_gam):
            result = run('NET001', 'invalid_type', 'yesterday')

        assert result is None

    def test_run_returns_empty_list_when_no_data(self):
        mock_gam = MagicMock()
        mock_gam.gam_revenue_by_domain.return_value = []

        with patch('services.dom_report_runner.GamService', return_value=mock_gam):
            result = run('NET001', 'domain', 'yesterday')

        assert result == []

    def test_run_returns_false_on_no_networks_error(self):
        mock_gam = MagicMock()
        mock_gam.gam_revenue_by_domain.side_effect = Exception("NO_NETWORKS_TO_ACCESS")

        with patch('services.dom_report_runner.GamService', return_value=mock_gam):
            result = run('NET001', 'domain', 'yesterday')

        assert result is False

    def test_run_returns_none_on_other_exception(self):
        mock_gam = MagicMock()
        mock_gam.gam_revenue_by_domain.side_effect = Exception("connection timeout")

        with patch('services.dom_report_runner.GamService', return_value=mock_gam):
            result = run('NET001', 'domain', 'yesterday')

        assert result is None

    def test_run_returns_none_on_data_is_none(self):
        mock_gam = MagicMock()
        mock_gam.gam_revenue_by_domain.return_value = None

        with patch('services.dom_report_runner.GamService', return_value=mock_gam):
            result = run('NET001', 'domain', 'yesterday')

        assert result == []
