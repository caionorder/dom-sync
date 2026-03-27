import csv
from io import StringIO


def csvToJson(lines):
    """Converte um arquivo CSV em JSON usando headers customizados."""
    if not lines or len(lines) < 2:
        return []

    # Mapeamento dos headers do CSV para os headers customizados
    header_mapping = {
        "Dimension.DATE": "date",
        "Dimension.AD_UNIT_NAME": "domain",
        "Dimension.CUSTOM_CRITERIA": "custom_criteria",
        "Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "total_impressions",
        "Column.TOTAL_LINE_ITEM_LEVEL_CLICKS": "total_clicks",
        "Column.TOTAL_LINE_ITEM_LEVEL_CTR": "total_ctr",
        "Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE": "total_revenue",
        "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS": "adx_impressions",
        "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS": "adx_clicks",
        "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_CTR": "adx_ctr",
        "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE": "adx_revenue",
        "Column.AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM": "adx_ecpm",
    }

    # Campos de metricas que precisam de conversao numerica
    metric_fields = [
        'total_impressions', 'total_clicks', 'total_ctr', 'total_revenue',
        'adx_impressions', 'adx_clicks', 'adx_ctr', 'adx_revenue', 'adx_ecpm',
    ]

    # Campos monetarios que vem em micro-unidades (dividir por 1.000.000)
    micro_fields = ['adx_ecpm', 'adx_revenue', 'total_revenue']

    # Campos de porcentagem
    pct_fields = ['total_ctr', 'adx_ctr']

    csv_reader = csv.DictReader(StringIO('\n'.join(lines)))

    json_data = []
    for row in csv_reader:
        mapped_row = {}

        for original_header, value in row.items():
            if original_header in header_mapping:
                new_header = header_mapping[original_header]
                mapped_row[new_header] = value

        # Trata custom_criteria separadamente (utm_campaign = key-value)
        if 'custom_criteria' in mapped_row:
            custom_criteria = mapped_row.pop('custom_criteria')
            if '=' in custom_criteria:
                parts = custom_criteria.split('=', 1)
                mapped_row['custom_key'] = parts[0]
                mapped_row['custom_value'] = parts[1]
            else:
                mapped_row['custom_key'] = custom_criteria
                mapped_row['custom_value'] = ''

        # Converte e formata valores numericos
        for field in metric_fields:
            if field in mapped_row:
                try:
                    value = float(mapped_row[field])

                    if field in micro_fields:
                        value = value / 1000000
                        mapped_row[field] = round(value, 2)
                    elif field in pct_fields:
                        mapped_row[field] = value
                    else:
                        mapped_row[field] = int(value)

                except ValueError:
                    pass

        json_data.append(mapped_row)

    return json_data
