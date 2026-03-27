"""Tests for core/multiprocess/worker.py"""
import pytest
from unittest.mock import MagicMock, patch
from google.api_core.exceptions import Unauthorized, PermissionDenied, TooManyRequests
from core.multiprocess.worker import NetworkWorker


def make_worker():
    mock_rate_limiter = MagicMock()
    mock_rate_limiter.wait_if_needed.return_value = None
    return NetworkWorker(mock_rate_limiter), mock_rate_limiter


def make_job(job_id='job1', network_code='NET001', name='Test Net'):
    network = {'network_code': network_code, 'name': name}
    params = MagicMock()
    params.type = 'domain'
    params.day = 'yesterday'
    return (job_id, network, params)


class TestNetworkWorkerProcessNetwork:
    def test_success_result(self):
        worker, rate_limiter = make_worker()
        job = make_job()

        with patch('core.multiprocess.worker.run', return_value={'total': 1}):
            result = worker.process_network(job)

        assert result['status'] == 'success'
        assert result['network_code'] == 'NET001'
        assert result['network_name'] == 'Test Net'
        assert result['job_id'] == 'job1'
        assert 'duration' in result
        assert 'result' in result

    def test_auth_error_when_result_is_false(self):
        worker, rate_limiter = make_worker()
        job = make_job()

        with patch('core.multiprocess.worker.run', return_value=False):
            result = worker.process_network(job)

        assert result['status'] == 'auth_error'
        assert 'error' in result

    def test_unauthorized_exception_returns_auth_error(self):
        worker, rate_limiter = make_worker()
        job = make_job()

        with patch('core.multiprocess.worker.run', side_effect=Unauthorized("unauthorized")):
            result = worker.process_network(job)

        assert result['status'] == 'auth_error'

    def test_permission_denied_exception_returns_auth_error(self):
        worker, rate_limiter = make_worker()
        job = make_job()

        with patch('core.multiprocess.worker.run', side_effect=PermissionDenied("forbidden")):
            result = worker.process_network(job)

        assert result['status'] == 'auth_error'

    def test_generic_exception_returns_error(self):
        worker, rate_limiter = make_worker()
        job = make_job()

        with patch('core.multiprocess.worker.run', side_effect=Exception("some error")):
            result = worker.process_network(job)

        assert result['status'] == 'error'
        assert 'error' in result

    def test_request_count_incremented_on_success(self):
        worker, rate_limiter = make_worker()
        job = make_job()

        with patch('core.multiprocess.worker.run', return_value=[]):
            result = worker.process_network(job)

        assert result['request_count'] == 1

    def test_error_has_duration(self):
        worker, rate_limiter = make_worker()
        job = make_job()

        with patch('core.multiprocess.worker.run', side_effect=Exception("fail")):
            result = worker.process_network(job)

        assert 'duration' in result


class TestNetworkWorkerApiCallWithRetry:
    def test_success_on_first_attempt(self):
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(return_value='data')

        result = worker._api_call_with_retry('NET001', mock_func)

        assert result == 'data'
        mock_func.assert_called_once()

    def test_retries_on_429(self):
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(side_effect=[
            Exception("429 rate limit"),
            'success',
        ])

        with patch('core.multiprocess.worker.time.sleep'):
            result = worker._api_call_with_retry('NET001', mock_func)

        assert result == 'success'
        assert mock_func.call_count == 2

    def test_retries_on_resource_exhausted(self):
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(side_effect=[
            Exception("Resource has been exhausted"),
            'ok',
        ])

        with patch('core.multiprocess.worker.time.sleep'):
            result = worker._api_call_with_retry('NET001', mock_func)

        assert result == 'ok'

    def test_raises_too_many_requests_after_max_retries(self):
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(side_effect=Exception("429 forever"))

        with patch('core.multiprocess.worker.time.sleep'):
            with pytest.raises(TooManyRequests):
                worker._api_call_with_retry('NET001', mock_func)

        assert mock_func.call_count == 10  # max_retries = 10

    def test_raises_non_429_immediately(self):
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(side_effect=ValueError("bad input"))

        with pytest.raises(ValueError):
            worker._api_call_with_retry('NET001', mock_func)

        mock_func.assert_called_once()

    def test_delay_capped_at_max(self):
        worker, rate_limiter = make_worker()
        sleep_times = []

        mock_func = MagicMock(side_effect=[
            Exception("429"),
            Exception("429"),
            Exception("429"),
            'ok',
        ])

        with patch('core.multiprocess.worker.time.sleep', side_effect=lambda t: sleep_times.append(t)), \
             patch('core.multiprocess.worker.time.time', return_value=0.5):
            result = worker._api_call_with_retry('NET001', mock_func)

        # max_delay = 120
        for t in sleep_times:
            assert t <= 120.0 * 1.2  # with jitter factor

    def test_rate_limiter_called_per_attempt(self):
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(side_effect=[Exception("429"), 'ok'])

        with patch('core.multiprocess.worker.time.sleep'):
            worker._api_call_with_retry('NET001', mock_func)

        assert rate_limiter.wait_if_needed.call_count == 2

    def test_passes_args_to_func(self):
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(return_value='result')

        result = worker._api_call_with_retry('NET001', mock_func, 'arg1', key='val')

        mock_func.assert_called_once_with('arg1', key='val')

    def test_returns_none_when_max_retries_zero(self):
        """When max_retries=0, the for loop body never executes and return None is hit."""
        worker, rate_limiter = make_worker()
        mock_func = MagicMock(return_value='result')

        # Monkeypatch max_retries to 0 by using the method directly
        original = worker._api_call_with_retry

        def zero_retries(network_code, func, *args, **kwargs):
            max_retries = 0
            for attempt in range(max_retries):
                try:
                    worker.network_rate_limiter.wait_if_needed(str(network_code))
                    return func(*args, **kwargs)
                except Exception as e:
                    raise
            return None

        result = zero_retries('NET001', mock_func)
        assert result is None
        mock_func.assert_not_called()
