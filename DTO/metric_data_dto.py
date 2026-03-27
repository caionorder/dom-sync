from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class MetricDataDTO:
    """Data Transfer Object para metricas DOM (Revenue by Domain / UTM Campaign)"""

    domain: str
    network: str
    date: str
    impressions: int
    clicks: int
    ctr: float
    ecpm: float
    revenue: float
    utm_campaign: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MetricDataDTO':
        """Cria uma instancia a partir de um dicionario"""
        return cls(
            domain=data['domain'],
            network=data['network'],
            date=data['date'],
            impressions=int(data.get('impressions', 0)),
            clicks=int(data.get('clicks', 0)),
            ctr=float(data.get('ctr', 0.0)),
            ecpm=float(data.get('ecpm', 0.0)),
            revenue=float(data.get('revenue', 0.0)),
            utm_campaign=data.get('utm_campaign'),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionario"""
        data = {
            'domain': self.domain,
            'network': self.network,
            'date': self.date,
            'impressions': self.impressions,
            'clicks': self.clicks,
            'ctr': self.ctr,
            'ecpm': self.ecpm,
            'revenue': self.revenue,
        }

        if self.utm_campaign is not None:
            data['utm_campaign'] = self.utm_campaign

        return data
