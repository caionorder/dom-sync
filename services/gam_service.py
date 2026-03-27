import gzip
import io
import logging
import time
from datetime import datetime, timedelta, timezone

from googleads import ad_manager, errors, oauth2

from config.settings import Config
from helpers.jsonfy import csvToJson

logger = logging.getLogger(__name__)


class GamService:
    """Cliente SOAP para Google Ad Manager - relatórios DOM"""

    version = "v202511"

    def __init__(self, network_code, report_type=None):
        self.network_code = network_code
        self.json_key_path = Config.json_key_path

        if report_type:
            self.start = report_type.start
            self.end = report_type.end
        else:
            self.start = 1
            self.end = 0

    def auth(self):
        """Autentica e retorna um cliente do Google Ad Manager"""
        oauth2_client = oauth2.GoogleServiceAccountClient(
            key_file=self.json_key_path, scope="https://www.googleapis.com/auth/dfp"
        )

        ad_manager_client = ad_manager.AdManagerClient(
            oauth2_client=oauth2_client,
            application_name="JoinAds-DOM",
            network_code=self.network_code,
        )

        self.ad_manager_client = ad_manager_client
        return ad_manager_client

    def gam_revenue_by_domain(self):
        """Gera relatorio de revenue by domain"""
        client = self.auth()
        report_query = self._report_query_domain()
        report_service = self._report_service(client)
        response = self._report_run(report_query, report_service)
        return response

    def gam_revenue_by_utm_campaign(self):
        """Gera relatorio de revenue by utm_campaign (key-values)"""
        client = self.auth()
        report_query = self._report_query_utm_campaign()
        report_service = self._report_service(client)
        response = self._report_run(report_query, report_service)
        return response

    def _get_date_range(self):
        """Calcula as datas de inicio e fim do relatorio"""
        start_date = datetime.now(timezone(timedelta(hours=-3))) - timedelta(
            days=self.start
        )
        end_date = datetime.now(timezone(timedelta(hours=-3))) - timedelta(
            days=self.end
        )
        return start_date, end_date

    def _report_query_domain(self):
        """Cria query para relatorio de revenue by domain"""
        start_date, end_date = self._get_date_range()

        report_query = {
            "dimensions": ["DATE", "AD_UNIT_NAME"],
            "columns": [
                "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                "TOTAL_LINE_ITEM_LEVEL_CTR",
                "TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_CTR",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM",
            ],
            "reportCurrency": "USD",
            "dateRangeType": "CUSTOM_DATE",
            "adUnitView": "HIERARCHICAL",
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day,
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day,
            },
        }

        return report_query

    def _report_query_utm_campaign(self):
        """Cria query para relatorio de revenue by utm_campaign (key-values)"""
        start_date, end_date = self._get_date_range()

        report_query = {
            "dimensions": ["DATE", "AD_UNIT_NAME", "CUSTOM_CRITERIA"],
            "columns": [
                "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                "TOTAL_LINE_ITEM_LEVEL_CLICKS",
                "TOTAL_LINE_ITEM_LEVEL_CTR",
                "TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_CTR",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE",
                "AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM",
            ],
            "reportCurrency": "USD",
            "dateRangeType": "CUSTOM_DATE",
            "adUnitView": "HIERARCHICAL",
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day,
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day,
            },
        }

        return report_query

    def _report_service(self, client):
        """Cria servico de relatorios com retry"""
        max_retries = 5
        attempt = 0

        while attempt < max_retries:
            try:
                report_service = client.GetService(
                    "ReportService", version=self.version
                )
                return report_service

            except errors.GoogleAdsError as e:
                error_message = str(e)

                if "AuthenticationError.NO_NETWORKS_TO_ACCESS" in error_message:
                    break

                attempt += 1
                if attempt >= max_retries:
                    raise

                time.sleep(1)

        return None

    def _report_run(self, report_query, report_service):
        """Executa o relatorio e retorna os resultados"""
        report_job = {"reportQuery": report_query}

        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job["id"]

        # Aguarda a conclusao do relatorio
        while True:
            job_status = report_service.getReportJobStatus(report_job_id)
            if job_status == "COMPLETED":
                break
            elif job_status == "FAILED":
                raise Exception("Report job failed")
            time.sleep(0.5)

        try:
            report_data = io.BytesIO()

            report_downloader = report_service.getReportDownloadURL(
                report_job_id, "CSV_DUMP"
            )

            import ssl
            import urllib.request

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            req = urllib.request.Request(report_downloader)
            with urllib.request.urlopen(req, context=ssl_context) as response:
                compressed_data = response.read()

            with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
                resultado = gz.read().decode("utf-8")

            lines = resultado.split("\n")

            if len(lines) <= 1:
                return []

            response = csvToJson(lines)
            return response

        except Exception:
            raise
