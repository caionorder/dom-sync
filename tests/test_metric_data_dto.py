"""Tests for DTO/metric_data_dto.py"""
import pytest
from DTO.metric_data_dto import MetricDataDTO


class TestMetricDataDTOCreation:
    def test_direct_instantiation_with_all_fields(self):
        dto = MetricDataDTO(
            domain='example.com',
            network='123456',
            date='2024-01-15',
            impressions=1000,
            clicks=50,
            ctr=5.0,
            ecpm=2.5,
            revenue=2.50,
            utm_campaign='summer_sale',
        )
        assert dto.domain == 'example.com'
        assert dto.network == '123456'
        assert dto.date == '2024-01-15'
        assert dto.impressions == 1000
        assert dto.clicks == 50
        assert dto.ctr == 5.0
        assert dto.ecpm == 2.5
        assert dto.revenue == 2.50
        assert dto.utm_campaign == 'summer_sale'

    def test_direct_instantiation_without_utm_campaign(self):
        dto = MetricDataDTO(
            domain='example.com',
            network='123456',
            date='2024-01-15',
            impressions=500,
            clicks=10,
            ctr=2.0,
            ecpm=1.0,
            revenue=0.50,
        )
        assert dto.utm_campaign is None

    def test_utm_campaign_defaults_to_none(self):
        dto = MetricDataDTO(
            domain='test.com',
            network='999',
            date='2024-01-01',
            impressions=0,
            clicks=0,
            ctr=0.0,
            ecpm=0.0,
            revenue=0.0,
        )
        assert dto.utm_campaign is None


class TestMetricDataDTOFromDict:
    def test_from_dict_with_all_fields(self):
        data = {
            'domain': 'example.com',
            'network': '123456',
            'date': '2024-01-15',
            'impressions': '1000',
            'clicks': '50',
            'ctr': '5.0',
            'ecpm': '2.5',
            'revenue': '2.50',
            'utm_campaign': 'summer_sale',
        }
        dto = MetricDataDTO.from_dict(data)
        assert dto.domain == 'example.com'
        assert dto.network == '123456'
        assert dto.impressions == 1000
        assert dto.clicks == 50
        assert dto.ctr == 5.0
        assert dto.ecpm == 2.5
        assert dto.revenue == 2.50
        assert dto.utm_campaign == 'summer_sale'

    def test_from_dict_without_optional_fields(self):
        data = {
            'domain': 'example.com',
            'network': '123456',
            'date': '2024-01-15',
        }
        dto = MetricDataDTO.from_dict(data)
        assert dto.impressions == 0
        assert dto.clicks == 0
        assert dto.ctr == 0.0
        assert dto.ecpm == 0.0
        assert dto.revenue == 0.0
        assert dto.utm_campaign is None

    def test_from_dict_converts_numeric_strings(self):
        data = {
            'domain': 'test.com',
            'network': '999',
            'date': '2024-06-01',
            'impressions': '500',
            'clicks': '25',
            'ctr': '5.0',
            'ecpm': '3.14',
            'revenue': '1.57',
        }
        dto = MetricDataDTO.from_dict(data)
        assert isinstance(dto.impressions, int)
        assert isinstance(dto.clicks, int)
        assert isinstance(dto.ctr, float)
        assert isinstance(dto.ecpm, float)
        assert isinstance(dto.revenue, float)

    def test_from_dict_utm_campaign_none_when_missing(self):
        data = {
            'domain': 'test.com',
            'network': '999',
            'date': '2024-06-01',
        }
        dto = MetricDataDTO.from_dict(data)
        assert dto.utm_campaign is None

    def test_from_dict_utm_campaign_explicitly_none(self):
        data = {
            'domain': 'test.com',
            'network': '999',
            'date': '2024-06-01',
            'utm_campaign': None,
        }
        dto = MetricDataDTO.from_dict(data)
        assert dto.utm_campaign is None


class TestMetricDataDTOToDict:
    def test_to_dict_without_utm_campaign(self):
        dto = MetricDataDTO(
            domain='example.com',
            network='123456',
            date='2024-01-15',
            impressions=1000,
            clicks=50,
            ctr=5.0,
            ecpm=2.5,
            revenue=2.50,
        )
        result = dto.to_dict()
        assert result['domain'] == 'example.com'
        assert result['network'] == '123456'
        assert result['date'] == '2024-01-15'
        assert result['impressions'] == 1000
        assert result['clicks'] == 50
        assert result['ctr'] == 5.0
        assert result['ecpm'] == 2.5
        assert result['revenue'] == 2.50
        assert 'utm_campaign' not in result

    def test_to_dict_with_utm_campaign(self):
        dto = MetricDataDTO(
            domain='example.com',
            network='123456',
            date='2024-01-15',
            impressions=1000,
            clicks=50,
            ctr=5.0,
            ecpm=2.5,
            revenue=2.50,
            utm_campaign='black_friday',
        )
        result = dto.to_dict()
        assert result['utm_campaign'] == 'black_friday'

    def test_to_dict_does_not_include_utm_campaign_when_none(self):
        dto = MetricDataDTO(
            domain='example.com',
            network='123456',
            date='2024-01-15',
            impressions=0,
            clicks=0,
            ctr=0.0,
            ecpm=0.0,
            revenue=0.0,
            utm_campaign=None,
        )
        result = dto.to_dict()
        assert 'utm_campaign' not in result

    def test_roundtrip_from_dict_to_dict(self):
        original = {
            'domain': 'example.com',
            'network': '123456',
            'date': '2024-01-15',
            'impressions': '1000',
            'clicks': '50',
            'ctr': '5.0',
            'ecpm': '2.5',
            'revenue': '2.50',
            'utm_campaign': 'test_campaign',
        }
        dto = MetricDataDTO.from_dict(original)
        result = dto.to_dict()
        assert result['domain'] == original['domain']
        assert result['utm_campaign'] == original['utm_campaign']
