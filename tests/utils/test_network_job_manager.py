"""Tests for utils/network_job_manager.py"""
import pytest
import threading
from unittest.mock import patch, MagicMock
from utils.network_job_manager import NetworkJobManager


def make_job(job_id, network_code, name='Test Network'):
    network = {'network_code': network_code, 'name': name}
    params = MagicMock()
    return (job_id, network, params)


class TestNetworkJobManagerInit:
    def test_default_max_concurrent(self):
        manager = NetworkJobManager()
        assert manager.max_concurrent == 30

    def test_custom_max_concurrent(self):
        manager = NetworkJobManager(max_concurrent_networks=5)
        assert manager.max_concurrent == 5

    def test_initial_state_is_empty(self):
        manager = NetworkJobManager()
        assert manager.active_networks == set()
        assert manager.network_jobs == {}
        assert manager.completed_networks == set()


class TestNetworkJobManagerAddJobs:
    def test_add_single_job(self):
        manager = NetworkJobManager()
        jobs = [make_job(0, 'NET001')]
        manager.add_jobs(jobs)
        assert 'NET001' in manager.network_jobs
        assert len(manager.network_jobs['NET001']) == 1

    def test_add_multiple_jobs_same_network(self):
        manager = NetworkJobManager()
        jobs = [
            make_job(0, 'NET001'),
            make_job(1, 'NET001'),
        ]
        manager.add_jobs(jobs)
        assert len(manager.network_jobs['NET001']) == 2

    def test_add_multiple_jobs_different_networks(self):
        manager = NetworkJobManager()
        jobs = [
            make_job(0, 'NET001'),
            make_job(1, 'NET002'),
        ]
        manager.add_jobs(jobs)
        assert 'NET001' in manager.network_jobs
        assert 'NET002' in manager.network_jobs

    def test_network_added_to_queue_once(self):
        manager = NetworkJobManager()
        jobs = [
            make_job(0, 'NET001'),
            make_job(1, 'NET001'),
            make_job(2, 'NET001'),
        ]
        manager.add_jobs(jobs)
        # queue should only have 1 entry for NET001
        assert manager.job_queue.qsize() == 1


class TestNetworkJobManagerGetNextNetwork:
    def test_get_next_network_returns_network_and_jobs(self):
        manager = NetworkJobManager()
        jobs = [make_job(0, 'NET001')]
        manager.add_jobs(jobs)

        network_code, returned_jobs = manager.get_next_network()
        assert network_code == 'NET001'
        assert len(returned_jobs) == 1

    def test_get_next_network_returns_none_when_empty(self):
        manager = NetworkJobManager()
        network_code, jobs = manager.get_next_network()
        assert network_code is None
        assert jobs is None

    def test_get_next_network_marks_network_as_active(self):
        manager = NetworkJobManager()
        manager.add_jobs([make_job(0, 'NET001')])
        manager.get_next_network()
        assert 'NET001' in manager.active_networks

    def test_get_next_network_skips_already_active_network(self):
        manager = NetworkJobManager()
        manager.add_jobs([make_job(0, 'NET001')])
        manager.active_networks.add('NET001')

        with patch.object(manager.job_queue, 'get_nowait', side_effect=['NET001', Exception]):
            # Will try NET001, find it active, then raise (empty queue)
            network_code, jobs = manager.get_next_network()

        assert network_code is None

    def test_get_next_network_skips_completed_network(self):
        manager = NetworkJobManager()
        manager.add_jobs([make_job(0, 'NET001')])
        manager.completed_networks.add('NET001')

        with patch.object(manager.job_queue, 'get_nowait', side_effect=['NET001', Exception]):
            network_code, jobs = manager.get_next_network()

        assert network_code is None

    def test_get_next_network_waits_when_max_concurrent_reached(self):
        manager = NetworkJobManager(max_concurrent_networks=1)
        manager.add_jobs([make_job(0, 'NET001'), make_job(1, 'NET002')])
        manager.active_networks.add('NET002')

        call_count = [0]
        original_sleep = __import__('time').sleep

        def limited_sleep(t):
            call_count[0] += 1
            # After first sleep, clear active networks to allow exit
            manager.active_networks.clear()

        with patch('utils.network_job_manager.time') as mock_time:
            mock_time.sleep = limited_sleep

            # First call to get_next_network will hit max_concurrent, sleep once, then proceed
            manager.active_networks = {'BLOCKER'}
            manager.add_jobs([make_job(0, 'NET_NEW')])
            # Force the active networks to clear on first sleep
            import threading

            def clear_after_delay():
                import time as real_time
                real_time.sleep(0.05)
                manager.active_networks.clear()

            t = threading.Thread(target=clear_after_delay)
            t.start()
            result = manager.get_next_network()
            t.join()


class TestNetworkJobManagerMarkCompleted:
    def test_mark_completed_moves_to_completed_set(self):
        manager = NetworkJobManager()
        manager.add_jobs([make_job(0, 'NET001')])
        manager.get_next_network()

        manager.mark_completed('NET001')

        assert 'NET001' not in manager.active_networks
        assert 'NET001' in manager.completed_networks

    def test_mark_completed_removes_from_active(self):
        manager = NetworkJobManager()
        manager.add_jobs([make_job(0, 'NET001')])
        manager.get_next_network()
        assert 'NET001' in manager.active_networks

        manager.mark_completed('NET001')
        assert 'NET001' not in manager.active_networks


class TestNetworkJobManagerIsComplete:
    def test_is_complete_when_all_processed(self):
        manager = NetworkJobManager()
        jobs = [make_job(0, 'NET001')]
        manager.add_jobs(jobs)
        manager.get_next_network()
        manager.mark_completed('NET001')

        assert manager.is_complete() is True

    def test_not_complete_when_active_networks_remain(self):
        manager = NetworkJobManager()
        manager.add_jobs([make_job(0, 'NET001')])
        manager.get_next_network()

        # Not yet marked completed
        assert manager.is_complete() is False

    def test_not_complete_when_queue_not_empty(self):
        manager = NetworkJobManager()
        manager.add_jobs([make_job(0, 'NET001'), make_job(1, 'NET002')])

        # Only process first network
        manager.get_next_network()
        manager.mark_completed('NET001')

        assert manager.is_complete() is False

    def test_is_complete_empty_manager(self):
        manager = NetworkJobManager()
        # No jobs added — queue empty, no active, no network_jobs
        assert manager.is_complete() is True
