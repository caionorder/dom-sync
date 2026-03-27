"""Tests for core/multiprocess/progress.py"""
import pytest
import threading
from unittest.mock import MagicMock, patch, call
from io import StringIO
from core.multiprocess.progress import update_progress, print_final_report


class TestUpdateProgress:
    def make_stats(self):
        return {
            'total': 0,
            'success': 0,
            'error': 0,
            'auth_error': 0,
            'rate_limit': 0,
        }

    def test_increments_total(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'success'})

        assert stats['total'] == 1

    def test_increments_success(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'success'})

        assert stats['success'] == 1

    def test_increments_error(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'error'})

        assert stats['error'] == 1

    def test_increments_auth_error(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'auth_error'})

        assert stats['auth_error'] == 1

    def test_unknown_status_not_incremented(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'unknown_status'})

        assert stats['total'] == 1  # total always incremented
        assert stats['success'] == 0
        assert stats['error'] == 0

    def test_request_count_accumulated(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'success', 'request_count': 3})

        assert stats['total_requests'] == 3

    def test_request_count_accumulated_multiple_calls(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'success', 'request_count': 2})
        update_progress(pbar, stats, stats_lock, {'status': 'success', 'request_count': 5})

        assert stats['total_requests'] == 7

    def test_pbar_update_called(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'success'})

        pbar.update.assert_called_once_with(1)

    def test_pbar_set_description_called(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'success'})

        pbar.set_description.assert_called_once()

    def test_success_rate_in_description(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats['total'] = 9
        stats['success'] = 9
        stats_lock = threading.Lock()

        update_progress(pbar, stats, stats_lock, {'status': 'success'})

        description = pbar.set_description.call_args[0][0]
        assert '100.0%' in description

    def test_thread_safe_update(self):
        pbar = MagicMock()
        stats = self.make_stats()
        stats_lock = threading.Lock()
        threads = []

        for _ in range(10):
            t = threading.Thread(
                target=update_progress,
                args=(pbar, stats, stats_lock, {'status': 'success'})
            )
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert stats['total'] == 10
        assert stats['success'] == 10


class TestPrintFinalReport:
    def test_prints_report_header(self, capsys):
        stats = {
            'total': 10,
            'success': 8,
            'error': 1,
            'auth_error': 1,
            'rate_limit': 0,
        }
        print_final_report(stats, 60.0)
        captured = capsys.readouterr()
        assert 'Relatorio Final' in captured.out

    def test_prints_elapsed_time(self, capsys):
        stats = {
            'total': 5,
            'success': 5,
            'error': 0,
            'auth_error': 0,
            'rate_limit': 0,
        }
        print_final_report(stats, 30.5)
        captured = capsys.readouterr()
        assert '30.50' in captured.out

    def test_prints_success_rate(self, capsys):
        stats = {
            'total': 10,
            'success': 7,
            'error': 2,
            'auth_error': 1,
            'rate_limit': 0,
        }
        print_final_report(stats, 10.0)
        captured = capsys.readouterr()
        assert '70.0%' in captured.out

    def test_no_success_rate_when_total_zero(self, capsys):
        stats = {
            'total': 0,
            'success': 0,
            'error': 0,
            'auth_error': 0,
            'rate_limit': 0,
        }
        print_final_report(stats, 1.0)
        captured = capsys.readouterr()
        # Should not crash and should print report
        assert 'Relatorio Final' in captured.out

    def test_prints_requests_per_second_when_available(self, capsys):
        stats = {
            'total': 10,
            'success': 10,
            'error': 0,
            'auth_error': 0,
            'rate_limit': 0,
            'total_requests': 100,
        }
        print_final_report(stats, 10.0)
        captured = capsys.readouterr()
        assert '10.00' in captured.out  # 100 requests / 10 seconds

    def test_no_request_rate_when_no_total_requests(self, capsys):
        stats = {
            'total': 5,
            'success': 5,
            'error': 0,
            'auth_error': 0,
            'rate_limit': 0,
        }
        print_final_report(stats, 10.0)
        captured = capsys.readouterr()
        assert 'Requisicoes totais' not in captured.out
