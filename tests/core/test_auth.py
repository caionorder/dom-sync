"""Tests for core/auth.py"""
import pytest
from unittest.mock import MagicMock, patch
import os


class TestGetAuthenticatedNetworkService:
    def test_returns_existing_client_when_token_valid(self):
        """When token hasn't expired, return new client without refreshing."""
        mock_redis = MagicMock()
        mock_redis.get.return_value = str(9999999999)  # Far future

        mock_client = MagicMock()

        with patch('core.auth.redis', mock_redis), \
             patch('core.auth.time') as mock_time, \
             patch('core.auth.NetworkServiceClient', return_value=mock_client):
            mock_time.time.return_value = 1000.0
            from core.auth import get_authenticated_network_service
            result = get_authenticated_network_service()

        assert result is mock_client
        # Should NOT call set (no token refresh)
        mock_redis.set.assert_not_called()

    def test_creates_new_client_when_token_expired(self):
        """When token expired, create new client and refresh token."""
        mock_redis = MagicMock()
        # Token expired (expiry = 500, now = 1000)
        mock_redis.get.return_value = str(500)

        mock_client = MagicMock()

        with patch('core.auth.redis', mock_redis), \
             patch('core.auth.time') as mock_time, \
             patch('core.auth.NetworkServiceClient', return_value=mock_client):
            mock_time.time.return_value = 1000.0
            from core.auth import get_authenticated_network_service
            result = get_authenticated_network_service()

        assert result is mock_client
        # Should set new expiry
        assert mock_redis.set.call_count == 2

    def test_creates_new_client_when_no_token(self):
        """When no token in Redis, create new client and store token."""
        mock_redis = MagicMock()
        mock_redis.get.return_value = None

        mock_client = MagicMock()

        with patch('core.auth.redis', mock_redis), \
             patch('core.auth.time') as mock_time, \
             patch('core.auth.NetworkServiceClient', return_value=mock_client):
            mock_time.time.return_value = 1000.0
            from core.auth import get_authenticated_network_service
            result = get_authenticated_network_service()

        assert result is mock_client
        assert mock_redis.set.call_count == 2

    def test_sets_google_credentials_env_when_missing(self):
        """Sets GOOGLE_APPLICATION_CREDENTIALS if not already set."""
        mock_redis = MagicMock()
        mock_redis.get.return_value = None

        import core.auth as auth_module
        original_key_path = auth_module.Config.json_key_path
        auth_module.Config.json_key_path = '/tmp/key.json'

        env_without_creds = {k: v for k, v in os.environ.items()
                             if k != 'GOOGLE_APPLICATION_CREDENTIALS'}

        try:
            with patch('core.auth.redis', mock_redis), \
                 patch('core.auth.time') as mock_time, \
                 patch('core.auth.NetworkServiceClient', return_value=MagicMock()), \
                 patch.dict('os.environ', env_without_creds, clear=True):
                mock_time.time.return_value = 1000.0
                auth_module.get_authenticated_network_service()
                # Assert inside the with block (patch.dict restores on exit)
                assert os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') == '/tmp/key.json'
        finally:
            auth_module.Config.json_key_path = original_key_path

    def test_does_not_override_existing_credentials(self):
        """Does not overwrite GOOGLE_APPLICATION_CREDENTIALS if already set."""
        mock_redis = MagicMock()
        mock_redis.get.return_value = None

        existing_path = '/existing/path/key.json'

        import core.auth as auth_module
        with patch('core.auth.redis', mock_redis), \
             patch('core.auth.time') as mock_time, \
             patch('core.auth.NetworkServiceClient', return_value=MagicMock()), \
             patch.dict('os.environ', {'GOOGLE_APPLICATION_CREDENTIALS': existing_path}):
            mock_time.time.return_value = 1000.0
            auth_module.get_authenticated_network_service()
            # Assert inside the with block
            assert os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') == existing_path

    def test_token_expiry_key_and_refresh_key_set(self):
        """Verifies the correct Redis keys are used."""
        mock_redis = MagicMock()
        mock_redis.get.return_value = None

        with patch('core.auth.redis', mock_redis), \
             patch('core.auth.time') as mock_time, \
             patch('core.auth.NetworkServiceClient', return_value=MagicMock()):
            mock_time.time.return_value = 1000.0
            from core.auth import get_authenticated_network_service, TOKEN_EXPIRY_KEY, LAST_REFRESH_KEY
            get_authenticated_network_service()

        set_calls = [call[0] for call in mock_redis.set.call_args_list]
        keys_set = [c[0] for c in set_calls]
        assert TOKEN_EXPIRY_KEY in keys_set
        assert LAST_REFRESH_KEY in keys_set
