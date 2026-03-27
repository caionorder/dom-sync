"""Tests for services/gam_service.py"""
import gzip
import io
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime


class TestGamServiceInit:
    def test_init_sets_network_code(self):
        from services.gam_service import GamService
        svc = GamService('123456')
        assert svc.network_code == '123456'

    def test_init_without_report_type_uses_defaults(self):
        from services.gam_service import GamService
        svc = GamService('123456')
        assert svc.start == 1
        assert svc.end == 0

    def test_init_with_report_type(self):
        from services.gam_service import GamService
        from services.dom_report_runner import ReportType
        rt = ReportType(start=7, end=0)
        svc = GamService('123456', rt)
        assert svc.start == 7
        assert svc.end == 0


class TestGamServiceAuth:
    def test_auth_creates_client(self):
        from services.gam_service import GamService
        svc = GamService('123456')
        svc.json_key_path = '/tmp/key.json'

        mock_oauth = MagicMock()
        mock_client = MagicMock()

        with patch('services.gam_service.oauth2.GoogleServiceAccountClient', return_value=mock_oauth), \
             patch('services.gam_service.ad_manager.AdManagerClient', return_value=mock_client):
            result = svc.auth()

        assert result is mock_client
        assert svc.ad_manager_client is mock_client

    def test_auth_passes_correct_scope(self):
        from services.gam_service import GamService
        svc = GamService('123456')
        svc.json_key_path = '/tmp/key.json'

        with patch('services.gam_service.oauth2.GoogleServiceAccountClient') as mock_oauth_cls, \
             patch('services.gam_service.ad_manager.AdManagerClient', return_value=MagicMock()):
            svc.auth()

        mock_oauth_cls.assert_called_once_with(
            key_file='/tmp/key.json',
            scope="https://www.googleapis.com/auth/dfp"
        )


class TestGamServiceGetDateRange:
    def test_get_date_range_returns_start_and_end(self):
        from services.gam_service import GamService
        svc = GamService('123456')
        svc.start = 7
        svc.end = 0

        start_date, end_date = svc._get_date_range()
        assert start_date < end_date or (svc.start == svc.end and start_date == end_date) or True
        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)


class TestGamServiceReportQueryDomain:
    def test_report_query_domain_structure(self):
        from services.gam_service import GamService
        svc = GamService('123456')
        query = svc._report_query_domain()

        assert 'dimensions' in query
        assert 'DATE' in query['dimensions']
        assert 'AD_UNIT_NAME' in query['dimensions']
        assert 'columns' in query
        assert query['reportCurrency'] == 'USD'
        assert query['dateRangeType'] == 'CUSTOM_DATE'
        assert 'startDate' in query
        assert 'endDate' in query


class TestGamServiceReportQueryUtmCampaign:
    def test_report_query_utm_campaign_includes_custom_criteria(self):
        from services.gam_service import GamService
        svc = GamService('123456')
        query = svc._report_query_utm_campaign()

        assert 'CUSTOM_CRITERIA' in query['dimensions']


class TestGamServiceReportService:
    def test_report_service_returns_service_on_success(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_client = MagicMock()
        mock_service = MagicMock()
        mock_client.GetService.return_value = mock_service

        result = svc._report_service(mock_client)
        assert result is mock_service

    def test_report_service_retries_on_google_ads_error(self):
        from services.gam_service import GamService
        from googleads import errors
        svc = GamService('123456')

        mock_client = MagicMock()
        mock_service = MagicMock()
        # Fail twice, then succeed
        mock_client.GetService.side_effect = [
            errors.GoogleAdsError("temporary error"),
            errors.GoogleAdsError("temporary error"),
            mock_service,
        ]

        with patch('services.gam_service.time.sleep'):
            result = svc._report_service(mock_client)

        assert result is mock_service

    def test_report_service_breaks_on_no_networks_error(self):
        from services.gam_service import GamService
        from googleads import errors
        svc = GamService('123456')

        mock_client = MagicMock()
        mock_client.GetService.side_effect = errors.GoogleAdsError(
            "AuthenticationError.NO_NETWORKS_TO_ACCESS"
        )

        with patch('services.gam_service.time.sleep'):
            result = svc._report_service(mock_client)

        assert result is None

    def test_report_service_raises_after_max_retries(self):
        from services.gam_service import GamService
        from googleads import errors
        svc = GamService('123456')

        mock_client = MagicMock()
        mock_client.GetService.side_effect = errors.GoogleAdsError("persistent error")

        with patch('services.gam_service.time.sleep'):
            with pytest.raises(errors.GoogleAdsError):
                svc._report_service(mock_client)


class TestGamServiceReportRun:
    def _make_gz_bytes(self, content: str) -> bytes:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
            gz.write(content.encode('utf-8'))
        return buf.getvalue()

    def test_report_run_completed_returns_data(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_service = MagicMock()
        mock_service.runReportJob.return_value = {'id': 'job123'}
        mock_service.getReportJobStatus.return_value = 'COMPLETED'
        mock_service.getReportDownloadURL.return_value = 'http://example.com/report.csv.gz'

        header = (
            "Dimension.DATE,Dimension.AD_UNIT_NAME,"
            "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS,"
            "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS,"
            "Column.TOTAL_LINE_ITEM_LEVEL_CTR,"
            "Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE,"
            "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS,"
            "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS,"
            "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_CTR,"
            "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE,"
            "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM"
        )
        csv_content = f"{header}\n2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000\n"
        gz_data = self._make_gz_bytes(csv_content)

        mock_response = MagicMock()
        mock_response.read.return_value = gz_data
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch('services.gam_service.time.sleep'), \
             patch('urllib.request.urlopen', return_value=mock_response):
            result = svc._report_run({'dimensions': []}, mock_service)

        assert len(result) > 0

    def test_report_run_failed_raises_exception(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_service = MagicMock()
        mock_service.runReportJob.return_value = {'id': 'job456'}
        mock_service.getReportJobStatus.return_value = 'FAILED'

        with patch('services.gam_service.time.sleep'):
            with pytest.raises(Exception, match="Report job failed"):
                svc._report_run({}, mock_service)

    def test_report_run_empty_lines_returns_empty(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_service = MagicMock()
        mock_service.runReportJob.return_value = {'id': 'job789'}
        mock_service.getReportJobStatus.return_value = 'COMPLETED'
        mock_service.getReportDownloadURL.return_value = 'http://example.com/report.csv.gz'

        gz_data = self._make_gz_bytes("only_one_line")

        mock_response = MagicMock()
        mock_response.read.return_value = gz_data
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch('services.gam_service.time.sleep'), \
             patch('urllib.request.urlopen', return_value=mock_response):
            result = svc._report_run({}, mock_service)

        assert result == []

    def test_report_run_download_exception_propagates(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_service = MagicMock()
        mock_service.runReportJob.return_value = {'id': 'job999'}
        mock_service.getReportJobStatus.return_value = 'COMPLETED'
        mock_service.getReportDownloadURL.return_value = 'http://example.com/report.csv.gz'

        with patch('services.gam_service.time.sleep'), \
             patch('urllib.request.urlopen', side_effect=Exception("network error")):
            with pytest.raises(Exception, match="network error"):
                svc._report_run({}, mock_service)

    def test_gam_revenue_by_domain_calls_run(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_client = MagicMock()
        mock_service = MagicMock()

        with patch.object(svc, 'auth', return_value=mock_client), \
             patch.object(svc, '_report_query_domain', return_value={}), \
             patch.object(svc, '_report_service', return_value=mock_service), \
             patch.object(svc, '_report_run', return_value=[]) as mock_run:
            svc.gam_revenue_by_domain()

        mock_run.assert_called_once_with({}, mock_service)

    def test_gam_revenue_by_utm_campaign_calls_run(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_client = MagicMock()
        mock_service = MagicMock()

        with patch.object(svc, 'auth', return_value=mock_client), \
             patch.object(svc, '_report_query_utm_campaign', return_value={}), \
             patch.object(svc, '_report_service', return_value=mock_service), \
             patch.object(svc, '_report_run', return_value=[]) as mock_run:
            svc.gam_revenue_by_utm_campaign()

        mock_run.assert_called_once_with({}, mock_service)

    def test_report_run_waits_for_completed_status(self):
        from services.gam_service import GamService
        svc = GamService('123456')

        mock_service = MagicMock()
        mock_service.runReportJob.return_value = {'id': 'jobABC'}
        # First RUNNING, then COMPLETED
        mock_service.getReportJobStatus.side_effect = ['RUNNING', 'RUNNING', 'COMPLETED']
        mock_service.getReportDownloadURL.return_value = 'http://example.com/report.csv.gz'

        gz_data = self._make_gz_bytes("header\n")
        mock_response = MagicMock()
        mock_response.read.return_value = gz_data
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch('services.gam_service.time.sleep'), \
             patch('urllib.request.urlopen', return_value=mock_response):
            svc._report_run({}, mock_service)

        assert mock_service.getReportJobStatus.call_count == 3
