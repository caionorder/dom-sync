"""Tests for helpers/jsonfy.py"""
import pytest
from helpers.jsonfy import csvToJson


HEADER_LINE = (
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

HEADER_WITH_CRITERIA = (
    "Dimension.DATE,Dimension.AD_UNIT_NAME,Dimension.CUSTOM_CRITERIA,"
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


class TestCsvToJsonEdgeCases:
    def test_empty_input_returns_empty_list(self):
        assert csvToJson([]) == []

    def test_single_line_returns_empty_list(self):
        assert csvToJson(["header_only"]) == []

    def test_none_input_returns_empty_list(self):
        assert csvToJson(None) == []

    def test_header_only_returns_empty_list(self):
        assert csvToJson([HEADER_LINE]) == []


class TestCsvToJsonBasicConversion:
    def test_basic_row_maps_headers(self):
        data_line = "2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert len(result) == 1
        row = result[0]
        assert row['date'] == '2024-01-15'
        assert row['domain'] == 'example.com'

    def test_impressions_converted_to_int(self):
        data_line = "2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert result[0]['total_impressions'] == 1000
        assert result[0]['adx_impressions'] == 500
        assert isinstance(result[0]['total_impressions'], int)

    def test_clicks_converted_to_int(self):
        data_line = "2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert result[0]['total_clicks'] == 50
        assert isinstance(result[0]['total_clicks'], int)

    def test_ctr_kept_as_float(self):
        data_line = "2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert result[0]['total_ctr'] == 0.05
        assert isinstance(result[0]['total_ctr'], float)


class TestCsvToJsonMicroFields:
    def test_adx_revenue_divided_by_million(self):
        # adx_revenue value = 1000000 -> should become 1.0
        data_line = "2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert result[0]['adx_revenue'] == 1.0

    def test_adx_ecpm_divided_by_million(self):
        # adx_ecpm = 2000000 -> should become 2.0
        data_line = "2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert result[0]['adx_ecpm'] == 2.0

    def test_total_revenue_divided_by_million(self):
        # total_revenue = 2500000 -> should become 2.5
        data_line = "2024-01-15,example.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert result[0]['total_revenue'] == 2.5

    def test_micro_fields_rounded_to_2_decimals(self):
        # 1500001 / 1000000 = 1.500001 -> rounded to 1.5
        data_line = "2024-01-15,example.com,1000,50,0.05,1500001,500,10,0.02,1500001,1500001"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert result[0]['total_revenue'] == 1.5
        assert result[0]['adx_revenue'] == 1.5
        assert result[0]['adx_ecpm'] == 1.5


class TestCsvToJsonCustomCriteria:
    def test_custom_criteria_with_equals_sign(self):
        data_line = "2024-01-15,example.com,utm_campaign=summer_sale,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_WITH_CRITERIA, data_line]
        result = csvToJson(lines)
        assert result[0]['custom_key'] == 'utm_campaign'
        assert result[0]['custom_value'] == 'summer_sale'
        assert 'custom_criteria' not in result[0]

    def test_custom_criteria_without_equals_sign(self):
        data_line = "2024-01-15,example.com,some_criteria_no_equals,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_WITH_CRITERIA, data_line]
        result = csvToJson(lines)
        assert result[0]['custom_key'] == 'some_criteria_no_equals'
        assert result[0]['custom_value'] == ''

    def test_custom_criteria_with_multiple_equals(self):
        # Split on first '=' only
        data_line = "2024-01-15,example.com,key=val=extra,1000,50,0.05,2500000,500,10,0.02,1000000,2000000"
        lines = [HEADER_WITH_CRITERIA, data_line]
        result = csvToJson(lines)
        assert result[0]['custom_key'] == 'key'
        assert result[0]['custom_value'] == 'val=extra'


class TestCsvToJsonMultipleRows:
    def test_multiple_rows_parsed(self):
        lines = [
            HEADER_LINE,
            "2024-01-15,domain1.com,1000,50,0.05,2500000,500,10,0.02,1000000,2000000",
            "2024-01-16,domain2.com,2000,100,0.05,5000000,1000,20,0.02,2000000,4000000",
        ]
        result = csvToJson(lines)
        assert len(result) == 2
        assert result[0]['domain'] == 'domain1.com'
        assert result[1]['domain'] == 'domain2.com'

    def test_unknown_headers_ignored(self):
        """Headers not in header_mapping are silently ignored."""
        custom_header = "Dimension.DATE,Dimension.AD_UNIT_NAME,Unknown.COLUMN"
        data_line = "2024-01-15,example.com,some_value"
        lines = [custom_header, data_line]
        result = csvToJson(lines)
        assert len(result) == 1
        assert 'date' in result[0]
        assert 'domain' in result[0]
        assert 'Unknown.COLUMN' not in result[0]


class TestCsvToJsonInvalidNumericValues:
    def test_invalid_numeric_value_kept_as_string(self):
        """ValueError in float conversion is caught; field kept as-is."""
        data_line = "2024-01-15,example.com,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A"
        lines = [HEADER_LINE, data_line]
        result = csvToJson(lines)
        assert len(result) == 1
        # Fields with invalid values remain as strings
        assert result[0]['total_impressions'] == 'N/A'
