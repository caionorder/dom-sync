"""Tests for services/metric_report_service.py"""
import pytest
from services.metric_report_service import MetricsReportService


class TestMetricsReportServiceProcessDomainMetrics:
    def setup_method(self):
        self.service = MetricsReportService(network='NET001')

    def test_basic_domain_processing(self):
        items = [{
            'domain': 'example.com',
            'date': '2024-01-15',
            'total_impressions': 1000,
            'total_clicks': 50,
            'total_revenue': 2.5,
            'adx_impressions': 500,
            'adx_clicks': 20,
            'adx_revenue': 1.0,
        }]
        result = self.service.process_domain_metrics(items)
        assert len(result) == 1
        assert result[0]['domain'] == 'example.com'
        assert result[0]['network'] == 'NET001'
        assert result[0]['impressions'] == 1500   # 1000 + 500
        assert result[0]['clicks'] == 70          # 50 + 20
        assert result[0]['revenue'] == 3.5        # 2.5 + 1.0

    def test_filters_out_dash_domain(self):
        items = [{'domain': '-', 'date': '2024-01-15'}]
        result = self.service.process_domain_metrics(items)
        assert result == []

    def test_filters_out_not_set_domain(self):
        items = [{'domain': '(not set)', 'date': '2024-01-15'}]
        result = self.service.process_domain_metrics(items)
        assert result == []

    def test_filters_out_empty_domain(self):
        items = [{'domain': '', 'date': '2024-01-15'}]
        result = self.service.process_domain_metrics(items)
        assert result == []

    def test_filters_out_missing_domain(self):
        items = [{'date': '2024-01-15'}]
        result = self.service.process_domain_metrics(items)
        assert result == []

    def test_ctr_calculated_correctly(self):
        items = [{
            'domain': 'test.com',
            'date': '2024-01-15',
            'total_impressions': 1000,
            'total_clicks': 10,
            'total_revenue': 0,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_domain_metrics(items)
        # CTR = (10 / 1000) * 100 = 1.0
        assert result[0]['ctr'] == 1.0

    def test_ecpm_calculated_correctly(self):
        items = [{
            'domain': 'test.com',
            'date': '2024-01-15',
            'total_impressions': 1000,
            'total_clicks': 0,
            'total_revenue': 2.0,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_domain_metrics(items)
        # eCPM = (2.0 / 1000) * 1000 = 2.0
        assert result[0]['ecpm'] == 2.0

    def test_ctr_and_ecpm_zero_when_no_impressions(self):
        items = [{
            'domain': 'test.com',
            'date': '2024-01-15',
            'total_impressions': 0,
            'total_clicks': 0,
            'total_revenue': 0,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_domain_metrics(items)
        assert result[0]['ctr'] == 0.0
        assert result[0]['ecpm'] == 0.0

    def test_multiple_domains(self):
        items = [
            {
                'domain': 'a.com',
                'date': '2024-01-15',
                'total_impressions': 500,
                'total_clicks': 10,
                'total_revenue': 1.0,
                'adx_impressions': 0,
                'adx_clicks': 0,
                'adx_revenue': 0,
            },
            {
                'domain': '-',
                'date': '2024-01-15',
            },
            {
                'domain': 'b.com',
                'date': '2024-01-15',
                'total_impressions': 200,
                'total_clicks': 5,
                'total_revenue': 0.5,
                'adx_impressions': 0,
                'adx_clicks': 0,
                'adx_revenue': 0,
            },
        ]
        result = self.service.process_domain_metrics(items)
        assert len(result) == 2
        domains = [r['domain'] for r in result]
        assert 'a.com' in domains
        assert 'b.com' in domains

    def test_network_stored_as_string(self):
        service = MetricsReportService(network=12345)
        items = [{
            'domain': 'test.com',
            'date': '2024-01-15',
            'total_impressions': 100,
            'total_clicks': 5,
            'total_revenue': 0.1,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = service.process_domain_metrics(items)
        assert isinstance(result[0]['network'], str)

    def test_revenue_rounded_to_2_decimals(self):
        items = [{
            'domain': 'test.com',
            'date': '2024-01-15',
            'total_impressions': 100,
            'total_clicks': 1,
            'total_revenue': 1.00001,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0.00001,
        }]
        result = self.service.process_domain_metrics(items)
        assert result[0]['revenue'] == round(1.00001 + 0.00001, 2)

    def test_empty_items_returns_empty(self):
        result = self.service.process_domain_metrics([])
        assert result == []


class TestMetricsReportServiceProcessUtmCampaignMetrics:
    def setup_method(self):
        self.service = MetricsReportService(network='NET002')

    def test_filters_utm_campaign_key(self):
        items = [{
            'domain': 'example.com',
            'date': '2024-01-15',
            'custom_key': 'utm_campaign',
            'custom_value': 'summer_sale',
            'total_impressions': 500,
            'total_clicks': 10,
            'total_revenue': 1.0,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert len(result) == 1
        assert result[0]['utm_campaign'] == 'summer_sale'

    def test_ignores_non_utm_campaign_keys(self):
        items = [{
            'domain': 'example.com',
            'date': '2024-01-15',
            'custom_key': 'other_key',
            'custom_value': 'other_value',
            'total_impressions': 500,
            'total_clicks': 10,
            'total_revenue': 1.0,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert result == []

    def test_ignores_empty_custom_value(self):
        items = [{
            'domain': 'example.com',
            'date': '2024-01-15',
            'custom_key': 'utm_campaign',
            'custom_value': '',
            'total_impressions': 500,
            'total_clicks': 10,
            'total_revenue': 1.0,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert result == []

    def test_filters_dash_domain(self):
        items = [{
            'domain': '-',
            'date': '2024-01-15',
            'custom_key': 'utm_campaign',
            'custom_value': 'test',
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert result == []

    def test_filters_not_set_domain(self):
        items = [{
            'domain': '(not set)',
            'date': '2024-01-15',
            'custom_key': 'utm_campaign',
            'custom_value': 'test',
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert result == []

    def test_filters_empty_domain(self):
        items = [{
            'domain': '',
            'custom_key': 'utm_campaign',
            'custom_value': 'test',
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert result == []

    def test_filters_missing_domain(self):
        items = [{
            'custom_key': 'utm_campaign',
            'custom_value': 'test',
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert result == []

    def test_ctr_zero_when_no_impressions(self):
        items = [{
            'domain': 'test.com',
            'date': '2024-01-15',
            'custom_key': 'utm_campaign',
            'custom_value': 'promo',
            'total_impressions': 0,
            'total_clicks': 0,
            'total_revenue': 0,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert result[0]['ctr'] == 0.0
        assert result[0]['ecpm'] == 0.0

    def test_result_includes_utm_campaign_field(self):
        items = [{
            'domain': 'test.com',
            'date': '2024-01-15',
            'custom_key': 'utm_campaign',
            'custom_value': 'campaign_123',
            'total_impressions': 100,
            'total_clicks': 5,
            'total_revenue': 0.5,
            'adx_impressions': 0,
            'adx_clicks': 0,
            'adx_revenue': 0,
        }]
        result = self.service.process_utm_campaign_metrics(items)
        assert 'utm_campaign' in result[0]
        assert result[0]['utm_campaign'] == 'campaign_123'

    def test_empty_items_returns_empty(self):
        result = self.service.process_utm_campaign_metrics([])
        assert result == []
