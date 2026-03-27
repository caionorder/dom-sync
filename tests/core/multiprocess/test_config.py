"""Tests for core/multiprocess/config.py"""
import pytest
import sys
from unittest.mock import patch


class TestGetArgs:
    def test_run_flag(self):
        with patch('sys.argv', ['prog', '--run']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.run is True

    def test_no_run_flag_defaults_false(self):
        with patch('sys.argv', ['prog']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.run is False

    def test_type_domain(self):
        with patch('sys.argv', ['prog', '--type', 'domain']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.type == 'domain'

    def test_type_utm_campaign(self):
        with patch('sys.argv', ['prog', '--type', 'utm_campaign']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.type == 'utm_campaign'

    def test_day_argument(self):
        with patch('sys.argv', ['prog', '--day', 'yesterday']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.day == 'yesterday'

    def test_workers_argument(self):
        with patch('sys.argv', ['prog', '--workers', '4']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.workers == 4

    def test_workers_default_is_cpu_count(self):
        import multiprocessing as mp
        with patch('sys.argv', ['prog']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.workers == mp.cpu_count()

    def test_limit_argument(self):
        with patch('sys.argv', ['prog', '--limit', '10']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.limit == 10

    def test_limit_default_none(self):
        with patch('sys.argv', ['prog']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.limit is None

    def test_debug_flag(self):
        with patch('sys.argv', ['prog', '--debug']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.debug is True

    def test_network_argument(self):
        with patch('sys.argv', ['prog', '--network', '123456']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.network == '123456'

    def test_network_default_none(self):
        with patch('sys.argv', ['prog']):
            from core.multiprocess.config import get_args
            args = get_args()
        assert args.network is None

    def test_type_invalid_choice_raises(self):
        with patch('sys.argv', ['prog', '--type', 'invalid']):
            from core.multiprocess.config import get_args
            with pytest.raises(SystemExit):
                get_args()
