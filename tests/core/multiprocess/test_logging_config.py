"""Tests for core/multiprocess/logging_config.py"""
import logging
import os
import pytest
from unittest.mock import patch, MagicMock


class TestSetupLogging:
    def teardown_method(self):
        """Clean up any log files created during tests."""
        if os.path.exists('dom_processing.log'):
            try:
                os.remove('dom_processing.log')
            except OSError:
                pass

    def test_returns_logger(self):
        from core.multiprocess.logging_config import setup_logging
        logger = setup_logging(debug=False)
        assert logger is not None

    def test_info_level_when_debug_false(self):
        from core.multiprocess.logging_config import setup_logging
        logger = setup_logging(debug=False)
        assert logger.level == logging.INFO

    def test_debug_level_when_debug_true(self):
        from core.multiprocess.logging_config import setup_logging
        logger = setup_logging(debug=True)
        assert logger.level == logging.DEBUG

    def test_adds_file_handler(self):
        from core.multiprocess.logging_config import setup_logging
        logger = setup_logging(debug=False)
        handler_types = [type(h).__name__ for h in logger.handlers]
        assert 'FileHandler' in handler_types

    def test_removes_existing_handlers(self):
        from core.multiprocess.logging_config import setup_logging
        root_logger = logging.getLogger()
        # Add a dummy handler
        dummy = logging.StreamHandler()
        root_logger.addHandler(dummy)

        setup_logging(debug=False)

        # Should not contain the old StreamHandler (it was removed)
        assert dummy not in root_logger.handlers

    def test_urllib3_silenced(self):
        from core.multiprocess.logging_config import setup_logging
        setup_logging()
        assert logging.getLogger('urllib3').level == logging.WARNING

    def test_google_silenced(self):
        from core.multiprocess.logging_config import setup_logging
        setup_logging()
        assert logging.getLogger('google').level == logging.WARNING

    def test_thread_formatter_includes_thread_id(self):
        """ThreadFormatter adds thread_id to log records."""
        import threading
        from core.multiprocess.logging_config import setup_logging

        logger = setup_logging(debug=False)

        # Get the formatter from file handler
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) > 0

        formatter = file_handlers[0].formatter
        assert formatter is not None

        # Create a log record and format it
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='test message',
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert str(threading.current_thread().ident) in formatted
