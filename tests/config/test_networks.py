"""Tests for config/networks.py"""
import pytest
from unittest.mock import patch


class TestGetEnabledNetworks:
    def test_returns_only_enabled_networks(self):
        mock_networks = [
            {'network_code': '111', 'name': 'Net A', 'enabled': True},
            {'network_code': '222', 'name': 'Net B', 'enabled': False},
            {'network_code': '333', 'name': 'Net C', 'enabled': True},
        ]
        with patch('config.networks.NETWORKS', mock_networks):
            from config.networks import get_enabled_networks
            result = get_enabled_networks()
        assert len(result) == 2
        codes = [n['network_code'] for n in result]
        assert '111' in codes
        assert '333' in codes
        assert '222' not in codes

    def test_returns_all_when_all_enabled(self):
        mock_networks = [
            {'network_code': '111', 'name': 'Net A', 'enabled': True},
            {'network_code': '222', 'name': 'Net B', 'enabled': True},
        ]
        with patch('config.networks.NETWORKS', mock_networks):
            from config.networks import get_enabled_networks
            result = get_enabled_networks()
        assert len(result) == 2

    def test_returns_empty_when_none_enabled(self):
        mock_networks = [
            {'network_code': '111', 'name': 'Net A', 'enabled': False},
        ]
        with patch('config.networks.NETWORKS', mock_networks):
            from config.networks import get_enabled_networks
            result = get_enabled_networks()
        assert result == []

    def test_enabled_defaults_to_true_when_missing(self):
        """Networks without 'enabled' key should be treated as enabled."""
        mock_networks = [
            {'network_code': '111', 'name': 'Net A'},
        ]
        with patch('config.networks.NETWORKS', mock_networks):
            from config.networks import get_enabled_networks
            result = get_enabled_networks()
        assert len(result) == 1

    def test_empty_networks_list(self):
        with patch('config.networks.NETWORKS', []):
            from config.networks import get_enabled_networks
            result = get_enabled_networks()
        assert result == []


class TestGetNetworkByCode:
    def test_finds_existing_network(self):
        mock_networks = [
            {'network_code': '111', 'name': 'Net A', 'enabled': True},
            {'network_code': '222', 'name': 'Net B', 'enabled': True},
        ]
        with patch('config.networks.NETWORKS', mock_networks):
            from config.networks import get_network_by_code
            result = get_network_by_code('111')
        assert result is not None
        assert result['name'] == 'Net A'

    def test_returns_none_for_missing_code(self):
        mock_networks = [
            {'network_code': '111', 'name': 'Net A', 'enabled': True},
        ]
        with patch('config.networks.NETWORKS', mock_networks):
            from config.networks import get_network_by_code
            result = get_network_by_code('999')
        assert result is None

    def test_returns_none_for_empty_networks(self):
        with patch('config.networks.NETWORKS', []):
            from config.networks import get_network_by_code
            result = get_network_by_code('111')
        assert result is None

    def test_returns_first_match(self):
        mock_networks = [
            {'network_code': '111', 'name': 'First Match', 'enabled': True},
            {'network_code': '111', 'name': 'Second Match', 'enabled': True},
        ]
        with patch('config.networks.NETWORKS', mock_networks):
            from config.networks import get_network_by_code
            result = get_network_by_code('111')
        assert result['name'] == 'First Match'
