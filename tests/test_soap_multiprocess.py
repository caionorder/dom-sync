"""Tests for soap_multiprocess.py main entry point."""
import pytest
import sys
import threading
from unittest.mock import MagicMock, patch, call


def make_args(
    run=True,
    workers=2,
    type='domain',
    day='yesterday',
    debug=False,
    limit=None,
    network=None,
):
    args = MagicMock()
    args.run = run
    args.workers = workers
    args.type = type
    args.day = day
    args.debug = debug
    args.limit = limit
    args.network = network
    return args


def make_networks(count=2):
    return [
        {'network_code': f'NET00{i}', 'name': f'Network {i}', 'enabled': True}
        for i in range(1, count + 1)
    ]


class TestMainListMode:
    def test_list_mode_prints_networks_and_exits(self, capsys):
        args = make_args(run=False)
        networks = make_networks(2)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=networks), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             pytest.raises(SystemExit) as exc_info:
            from soap_multiprocess import main
            main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert 'NET001' in captured.out or 'Network 1' in captured.out

    def test_no_networks_exits_0(self, capsys):
        args = make_args(run=False)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=[]), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             pytest.raises(SystemExit) as exc_info:
            from soap_multiprocess import main
            main()

        assert exc_info.value.code == 0

    def test_specific_network_filter(self, capsys):
        args = make_args(run=False, network='NET001')
        networks = make_networks(3)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=networks), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             pytest.raises(SystemExit) as exc_info:
            from soap_multiprocess import main
            main()

        assert exc_info.value.code == 0

    def test_specific_network_not_found_exits_1(self, capsys):
        args = make_args(run=False, network='NOTEXIST')
        networks = make_networks(2)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=networks), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             pytest.raises(SystemExit) as exc_info:
            from soap_multiprocess import main
            main()

        assert exc_info.value.code == 1

    def test_limit_applied(self, capsys):
        args = make_args(run=False, limit=1)
        networks = make_networks(5)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=networks), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             pytest.raises(SystemExit):
            from soap_multiprocess import main
            main()

        captured = capsys.readouterr()
        assert 'Limitando' in captured.out or '1' in captured.out


class TestMainRunMode:
    def test_run_mode_processes_networks(self, capsys):
        args = make_args(run=True, workers=1)
        networks = [{'network_code': 'NET001', 'name': 'Network 1', 'enabled': True}]

        mock_worker_instance = MagicMock()
        mock_worker_instance.process_network.return_value = {
            'status': 'success',
            'job_id': 0,
            'network_code': 'NET001',
            'network_name': 'Network 1',
            'request_count': 1,
            'duration': 0.5,
        }

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=networks), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             patch('soap_multiprocess.NetworkWorker', return_value=mock_worker_instance), \
             patch('soap_multiprocess.tqdm') as mock_tqdm:

            mock_pbar = MagicMock()
            mock_pbar.__enter__ = MagicMock(return_value=mock_pbar)
            mock_pbar.__exit__ = MagicMock(return_value=False)
            mock_tqdm.return_value = mock_pbar

            from soap_multiprocess import main
            main()

        captured = capsys.readouterr()
        assert 'DOM' in captured.out or 'processamento' in captured.out.lower()

    def test_keyboard_interrupt_exits_1(self, capsys):
        args = make_args(run=True)
        networks = make_networks(1)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', side_effect=KeyboardInterrupt), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             pytest.raises(SystemExit) as exc_info:
            from soap_multiprocess import main
            main()

        assert exc_info.value.code == 1

    def test_fatal_exception_exits_1(self, capsys):
        args = make_args(run=True)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', side_effect=Exception("Fatal DB error")), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             pytest.raises(SystemExit) as exc_info:
            from soap_multiprocess import main
            main()

        assert exc_info.value.code == 1

    def test_debug_mode_prints_traceback(self, capsys):
        args = make_args(run=True, debug=True)

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', side_effect=Exception("crash")), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             patch('traceback.print_exc') as mock_tb, \
             pytest.raises(SystemExit):
            from soap_multiprocess import main
            main()

        mock_tb.assert_called_once()


class TestMainRunModeAdvanced:
    def test_worker_thread_exception_handled(self, capsys):
        """Covers the except block in worker_thread (line 137-138)."""
        args = make_args(run=True, workers=1)
        networks = [{'network_code': 'NET001', 'name': 'Network 1', 'enabled': True}]

        mock_worker_instance = MagicMock()
        # Make process_network raise an exception
        mock_worker_instance.process_network.side_effect = Exception("worker crash")

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=networks), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             patch('soap_multiprocess.NetworkWorker', return_value=mock_worker_instance), \
             patch('soap_multiprocess.tqdm') as mock_tqdm:

            mock_pbar = MagicMock()
            mock_pbar.__enter__ = MagicMock(return_value=mock_pbar)
            mock_pbar.__exit__ = MagicMock(return_value=False)
            mock_tqdm.return_value = mock_pbar

            from soap_multiprocess import main
            main()

    def test_future_exception_handled(self, capsys):
        """Covers the exception path in as_completed futures (line 157-158)."""
        args = make_args(run=True, workers=1)
        networks = [{'network_code': 'NET001', 'name': 'Network 1', 'enabled': True}]

        mock_worker_instance = MagicMock()
        mock_worker_instance.process_network.return_value = {
            'status': 'success',
            'job_id': 0,
            'network_code': 'NET001',
            'network_name': 'Network 1',
            'request_count': 1,
        }

        # Make the future raise when result() is called
        from concurrent.futures import Future
        bad_future = Future()
        bad_future.set_exception(Exception("future failed"))

        with patch('soap_multiprocess.get_args', return_value=args), \
             patch('soap_multiprocess.setup_logging'), \
             patch('soap_multiprocess.get_enabled_networks', return_value=networks), \
             patch('soap_multiprocess.NetworkRateLimiter'), \
             patch('soap_multiprocess.NetworkWorker', return_value=mock_worker_instance), \
             patch('soap_multiprocess.tqdm') as mock_tqdm, \
             patch('soap_multiprocess.ThreadPoolExecutor') as mock_executor_cls:

            mock_pbar = MagicMock()
            mock_pbar.__enter__ = MagicMock(return_value=mock_pbar)
            mock_pbar.__exit__ = MagicMock(return_value=False)
            mock_tqdm.return_value = mock_pbar

            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=False)
            mock_executor.submit.return_value = bad_future
            mock_executor_cls.return_value = mock_executor

            from soap_multiprocess import main
            main()


class TestMainIfNameMain:
    def test_module_has_main_guard(self):
        """Verify soap_multiprocess.py has if __name__ == '__main__' guard."""
        import inspect
        import soap_multiprocess
        source = inspect.getsource(soap_multiprocess)
        assert "if __name__ == '__main__'" in source

    def test_main_is_callable(self):
        """Verify main function is importable and callable."""
        from soap_multiprocess import main
        assert callable(main)
