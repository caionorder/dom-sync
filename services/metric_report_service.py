from typing import Optional, Dict, List, Any
from DTO.metric_data_dto import MetricDataDTO
import logging

logger = logging.getLogger(__name__)


class MetricsReportService:
    """Servico de processamento e enriquecimento de metricas DOM"""

    def __init__(self, network: str):
        self.network = network

    def process_domain_metrics(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processa metricas de revenue by domain.
        Agrega total + adx impressions/clicks/revenue e calcula CTR e eCPM.
        """
        result = []

        for row in items:
            domain = row.get('domain', '')
            if not domain or domain == '-' or domain == '(not set)':
                continue

            # Agrega metricas total + adx
            impressions = int(row.get('total_impressions', 0)) + int(row.get('adx_impressions', 0))
            clicks = int(row.get('total_clicks', 0)) + int(row.get('adx_clicks', 0))
            revenue = float(row.get('total_revenue', 0)) + float(row.get('adx_revenue', 0))

            # Calcula CTR e eCPM agregados
            ctr = round((clicks / impressions * 100), 2) if impressions > 0 else 0.0
            ecpm = round((revenue / impressions * 1000), 2) if impressions > 0 else 0.0

            metric = MetricDataDTO(
                domain=domain,
                network=str(self.network),
                date=row.get('date', ''),
                impressions=impressions,
                clicks=clicks,
                ctr=ctr,
                ecpm=ecpm,
                revenue=round(revenue, 2),
            )

            result.append(metric.to_dict())

        return result

    def process_utm_campaign_metrics(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processa metricas de revenue by utm_campaign.
        Filtra apenas items com custom_key = 'utm_campaign'.
        """
        result = []

        for row in items:
            domain = row.get('domain', '')
            if not domain or domain == '-' or domain == '(not set)':
                continue

            # Filtra somente utm_campaign key-values
            custom_key = row.get('custom_key', '')
            custom_value = row.get('custom_value', '')

            if custom_key != 'utm_campaign' or not custom_value:
                continue

            # Agrega metricas total + adx
            impressions = int(row.get('total_impressions', 0)) + int(row.get('adx_impressions', 0))
            clicks = int(row.get('total_clicks', 0)) + int(row.get('adx_clicks', 0))
            revenue = float(row.get('total_revenue', 0)) + float(row.get('adx_revenue', 0))

            # Calcula CTR e eCPM agregados
            ctr = round((clicks / impressions * 100), 2) if impressions > 0 else 0.0
            ecpm = round((revenue / impressions * 1000), 2) if impressions > 0 else 0.0

            metric = MetricDataDTO(
                domain=domain,
                network=str(self.network),
                date=row.get('date', ''),
                impressions=impressions,
                clicks=clicks,
                ctr=ctr,
                ecpm=ecpm,
                revenue=round(revenue, 2),
                utm_campaign=custom_value,
            )

            result.append(metric.to_dict())

        return result
