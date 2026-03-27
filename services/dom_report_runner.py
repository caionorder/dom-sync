import re
import logging
from services.gam_service import GamService
from services.metric_report_service import MetricsReportService
from services.process_metrics import MetricsProcessor

logger = logging.getLogger(__name__)


class ReportType:
    def __init__(self, start, end):
        self.start = start  # dias atras para inicio
        self.end = end


def parse_day_parameter(day):
    """Parse do parametro day, suportando formatos dinamicos como last_X_days"""
    if day == "yesterday":
        return ReportType(start=1, end=1)
    elif day == "today":
        return ReportType(start=0, end=0)
    elif day == "last_7_days":
        return ReportType(start=7, end=0)
    elif day == "last_30_days":
        return ReportType(start=30, end=0)
    else:
        # Tenta parsear formato dinamico: last_X_days
        match = re.match(r'^last_(\d+)_days$', day)
        if match:
            days = int(match.group(1))
            return ReportType(start=days, end=0)
        return None


def run(network, report_type, day):
    """Executa um relatorio DOM com rate limiting"""

    period = parse_day_parameter(day)
    if period is None:
        logger.error(f"Periodo invalido: {day}")
        return None

    # 1. Criar o servico GAM
    gam_service = GamService(network, period)

    # 2. Executar o relatorio baseado no tipo
    try:
        if report_type == "domain":
            data = gam_service.gam_revenue_by_domain()
        elif report_type == "utm_campaign":
            data = gam_service.gam_revenue_by_utm_campaign()
        else:
            logger.error(f"Tipo de relatorio invalido: {report_type}")
            return None
    except Exception as e:
        # Verifica se eh erro de autenticacao
        if "NO_NETWORKS_TO_ACCESS" in str(e):
            logger.warning(f"Sem acesso a network {network}: {str(e)[:100]}")
            return False
        else:
            logger.error(f"Erro ao obter dados para a network {network}: {str(e)[:100]}")
            return None

    # Verifica se ha dados
    if not data or len(data) == 0:
        logger.warning(f"Nenhum dado retornado para a network {network}")
        return []

    # 3. Processar metricas
    metrics_service = MetricsReportService(network)

    if report_type == "domain":
        processed_data = metrics_service.process_domain_metrics(data)
    else:
        processed_data = metrics_service.process_utm_campaign_metrics(data)

    # 4. Salvar no banco usando o processador
    processor = MetricsProcessor()

    if report_type == "domain":
        stats = processor.process_domain_bulk(processed_data)
    else:
        stats = processor.process_utm_bulk(processed_data)

    return stats
