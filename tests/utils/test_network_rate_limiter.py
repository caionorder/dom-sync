"""Tests for utils/network_rate_limiter.py"""
import pytest
import threading
import time
from unittest.mock import patch, MagicMock


class TestNetworkRateLimiterSingleton:
    def test_singleton_returns_same_instance(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None

        inst1 = NetworkRateLimiter(requests_per_second=2)
        inst2 = NetworkRateLimiter(requests_per_second=5)
        assert inst1 is inst2

    def test_singleton_not_reinitialised_on_second_call(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None

        inst1 = NetworkRateLimiter(requests_per_second=3)
        assert inst1.requests_per_second == 3

        # Second instantiation should NOT change requests_per_second
        inst2 = NetworkRateLimiter(requests_per_second=10)
        assert inst2.requests_per_second == 3


class TestNetworkRateLimiterInit:
    def setup_method(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None

    def test_default_requests_per_second(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        limiter = NetworkRateLimiter()
        assert limiter.requests_per_second == 2

    def test_custom_requests_per_second(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        limiter = NetworkRateLimiter(requests_per_second=5)
        assert limiter.requests_per_second == 5

    def test_min_interval_calculated(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        limiter = NetworkRateLimiter(requests_per_second=4)
        assert limiter.min_interval == 0.25


class TestNetworkRateLimiterWaitIfNeeded:
    def setup_method(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None

    def test_first_request_not_blocked(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        limiter = NetworkRateLimiter(requests_per_second=2)

        with patch('utils.network_rate_limiter.time') as mock_time:
            mock_time.time.return_value = 1000.0
            mock_time.sleep = MagicMock()
            limiter.wait_if_needed('network_1')

        mock_time.sleep.assert_not_called()

    def test_requests_within_limit_not_blocked(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None
        limiter = NetworkRateLimiter(requests_per_second=2)

        with patch('utils.network_rate_limiter.time') as mock_time:
            mock_time.time.return_value = 1000.0
            mock_time.sleep = MagicMock()
            limiter.wait_if_needed('net_a')
            mock_time.time.return_value = 1000.1
            limiter.wait_if_needed('net_a')

        mock_time.sleep.assert_not_called()

    def test_exceeding_rate_limit_causes_sleep(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None
        limiter = NetworkRateLimiter(requests_per_second=2)

        with patch('utils.network_rate_limiter.time') as mock_time:
            t = 1000.0
            mock_time.time.return_value = t
            mock_time.sleep = MagicMock()

            # Fill the window with 2 requests at the same time
            limiter.request_windows['netX'] = [t, t]

            # 3rd request - both existing are within 1s window
            limiter.wait_if_needed('netX')

        mock_time.sleep.assert_called_once()

    def test_wait_time_not_called_when_negative(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None
        limiter = NetworkRateLimiter(requests_per_second=2)

        with patch('utils.network_rate_limiter.time') as mock_time:
            mock_time.sleep = MagicMock()
            # oldest request was more than 1 second ago — no wait needed
            mock_time.time.return_value = 1002.0
            limiter.request_windows['netY'] = [1000.0, 1000.5]
            limiter.wait_if_needed('netY')

        # oldest (1000.0) is 2s ago; window clears → no sleep
        mock_time.sleep.assert_not_called()

    def test_timestamps_appended_after_request(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None
        limiter = NetworkRateLimiter(requests_per_second=5)

        with patch('utils.network_rate_limiter.time') as mock_time:
            mock_time.time.return_value = 1000.0
            mock_time.sleep = MagicMock()
            limiter.wait_if_needed('netZ')

        assert len(limiter.request_windows['netZ']) == 1

    def test_different_networks_use_separate_windows(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None
        limiter = NetworkRateLimiter(requests_per_second=1)

        with patch('utils.network_rate_limiter.time') as mock_time:
            mock_time.time.return_value = 1000.0
            mock_time.sleep = MagicMock()
            limiter.wait_if_needed('net_A')

            # net_B should not be blocked even though net_A is at limit
            mock_time.time.return_value = 1000.0
            limiter.wait_if_needed('net_B')

        mock_time.sleep.assert_not_called()

    def test_get_lock_creates_per_network_lock(self):
        from utils.network_rate_limiter import NetworkRateLimiter
        NetworkRateLimiter._instance = None
        limiter = NetworkRateLimiter(requests_per_second=2)

        lock1 = limiter._get_lock('network_A')
        lock2 = limiter._get_lock('network_B')
        lock1_again = limiter._get_lock('network_A')

        assert lock1 is not lock2
        assert lock1 is lock1_again
