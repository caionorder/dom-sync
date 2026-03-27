"""Tests for utils/retry_handler.py"""
import pytest
from unittest.mock import MagicMock, patch
from utils.retry_handler import exponential_backoff_retry


class TestExponentialBackoffRetry:
    def test_success_on_first_attempt(self):
        mock_func = MagicMock(return_value='success')

        @exponential_backoff_retry(max_retries=3)
        def decorated():
            return mock_func()

        result = decorated()
        assert result == 'success'
        assert mock_func.call_count == 1

    def test_raises_non_429_immediately(self):
        mock_func = MagicMock(side_effect=ValueError("bad value"))

        @exponential_backoff_retry(max_retries=3)
        def decorated():
            return mock_func()

        with pytest.raises(ValueError):
            decorated()

        assert mock_func.call_count == 1

    def test_retries_on_429_error(self):
        mock_func = MagicMock(side_effect=[
            Exception("429 Too Many Requests"),
            'success',
        ])

        @exponential_backoff_retry(max_retries=3, initial_delay=0.01)
        def decorated():
            return mock_func()

        with patch('utils.retry_handler.time.sleep'), \
             patch('utils.retry_handler.random.random', return_value=0.5):
            result = decorated()

        assert result == 'success'
        assert mock_func.call_count == 2

    def test_retries_on_resource_exhausted_error(self):
        mock_func = MagicMock(side_effect=[
            Exception("Resource has been exhausted"),
            'ok',
        ])

        @exponential_backoff_retry(max_retries=3, initial_delay=0.01)
        def decorated():
            return mock_func()

        with patch('utils.retry_handler.time.sleep'), \
             patch('utils.retry_handler.random.random', return_value=0.5):
            result = decorated()

        assert result == 'ok'
        assert mock_func.call_count == 2

    def test_raises_after_max_retries_on_429(self):
        mock_func = MagicMock(side_effect=Exception("429 rate limit"))

        @exponential_backoff_retry(max_retries=3, initial_delay=0.01)
        def decorated():
            return mock_func()

        with patch('utils.retry_handler.time.sleep'), \
             patch('utils.retry_handler.random.random', return_value=0.5):
            with pytest.raises(Exception, match="429"):
                decorated()

        assert mock_func.call_count == 3

    def test_delay_doubles_on_each_retry(self):
        mock_func = MagicMock(side_effect=[
            Exception("429"),
            Exception("429"),
            'done',
        ])
        sleep_calls = []

        @exponential_backoff_retry(max_retries=5, initial_delay=1.0, max_delay=60.0, jitter=0.0)
        def decorated():
            return mock_func()

        with patch('utils.retry_handler.time.sleep', side_effect=lambda t: sleep_calls.append(t)), \
             patch('utils.retry_handler.random.random', return_value=0.5):
            result = decorated()

        assert result == 'done'
        # first delay = 1.0 * (1 + 0.0 * (0.5 * 2 - 1)) = 1.0
        # second delay = 2.0 * (1 + 0.0 * ...) = 2.0
        assert sleep_calls[0] == pytest.approx(1.0, abs=0.1)
        assert sleep_calls[1] == pytest.approx(2.0, abs=0.1)

    def test_delay_capped_at_max_delay(self):
        attempts = [Exception("429")] * 4 + ['ok']
        mock_func = MagicMock(side_effect=attempts)
        sleep_calls = []

        @exponential_backoff_retry(max_retries=5, initial_delay=10.0, max_delay=15.0, jitter=0.0)
        def decorated():
            return mock_func()

        with patch('utils.retry_handler.time.sleep', side_effect=lambda t: sleep_calls.append(t)), \
             patch('utils.retry_handler.random.random', return_value=0.5):
            decorated()

        # delay after 3rd retry would be 10 * 2^2 = 40, but capped at 15
        for d in sleep_calls[2:]:
            assert d <= 15.0

    def test_wraps_preserves_function_name(self):
        @exponential_backoff_retry(max_retries=3)
        def my_function():
            pass

        assert my_function.__name__ == 'my_function'

    def test_passes_args_and_kwargs(self):
        mock_func = MagicMock(return_value='result')

        @exponential_backoff_retry(max_retries=3)
        def decorated(a, b, key=None):
            return mock_func(a, b, key=key)

        result = decorated(1, 2, key='val')
        mock_func.assert_called_once_with(1, 2, key='val')
        assert result == 'result'
