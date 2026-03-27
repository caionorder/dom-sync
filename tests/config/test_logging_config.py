"""Tests for config/logging_config.py"""
import logging
import pytest


class TestConfigureLogging:
    def test_configure_logging_sets_critical_for_googleads(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('googleads').level == logging.CRITICAL

    def test_configure_logging_sets_critical_for_googleads_common(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('googleads.common').level == logging.CRITICAL

    def test_configure_logging_sets_critical_for_googleads_errors(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('googleads.errors').level == logging.CRITICAL

    def test_configure_logging_sets_critical_for_googleads_soap(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('googleads.soap').level == logging.CRITICAL

    def test_configure_logging_sets_critical_for_zeep(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('zeep').level == logging.CRITICAL

    def test_configure_logging_sets_critical_for_zeep_transports(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('zeep.transports').level == logging.CRITICAL

    def test_configure_logging_sets_critical_for_urllib3(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('urllib3').level == logging.CRITICAL

    def test_configure_logging_sets_critical_for_requests(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger('requests').level == logging.CRITICAL

    def test_configure_logging_sets_root_to_warning(self):
        from config.logging_config import configure_logging
        configure_logging()
        assert logging.getLogger().level == logging.WARNING

    def test_module_auto_configures_on_import(self):
        """The module calls configure_logging() at import time."""
        import importlib
        import config.logging_config as lc
        importlib.reload(lc)
        assert logging.getLogger('googleads').level == logging.CRITICAL
