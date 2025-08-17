import pytest
from datetime import datetime

from src.sec_edgar_parser.utils.date_utils import (
    parse_date_flexible,
    format_date_for_display,
    is_valid_year,
    get_filing_year_range,
)
from src.sec_edgar_parser.utils.text_utils import (
    normalize_text,
    find_best_match,
    extract_xbrl_element_name,
    extract_numeric_value,
    is_numeric_cell,
)


class TestDateUtils:
    def test_parse_date_flexible_iso_format(self):
        result = parse_date_flexible("2023-12-31")
        assert result == datetime(2023, 12, 31)
    
    def test_parse_date_flexible_us_format(self):
        result = parse_date_flexible("12/31/2023")
        assert result == datetime(2023, 12, 31)
    
    def test_parse_date_flexible_text_format(self):
        result = parse_date_flexible("December 31, 2023")
        assert result == datetime(2023, 12, 31)
    
    def test_parse_date_flexible_datetime_input(self):
        dt = datetime(2023, 12, 31)
        result = parse_date_flexible(dt)
        assert result == dt
    
    def test_parse_date_flexible_none_input(self):
        result = parse_date_flexible(None)
        assert result is None
    
    def test_parse_date_flexible_invalid_input(self):
        result = parse_date_flexible("invalid date")
        assert result is None
    
    def test_format_date_for_display(self):
        dt = datetime(2023, 12, 31)
        result = format_date_for_display(dt)
        assert result == "31-12-2023"
    
    def test_format_date_for_display_custom_format(self):
        dt = datetime(2023, 12, 31)
        result = format_date_for_display(dt, "%Y-%m-%d")
        assert result == "2023-12-31"
    
    def test_is_valid_year(self):
        assert is_valid_year(1993) is True
        assert is_valid_year(2023) is True
        assert is_valid_year(2100) is True
        assert is_valid_year(1992) is False
        assert is_valid_year(2101) is False
    
    def test_get_filing_year_range(self):
        min_year, max_year = get_filing_year_range()
        assert min_year == 1993
        assert max_year > 2024  # Should be current year + 1


class TestTextUtils:
    def test_normalize_text(self):
        result = normalize_text("  Consolidated Statements of Income  ")
        assert result == "consolidated statements of income"
    
    def test_normalize_text_with_punctuation(self):
        result = normalize_text("Income Statement (Unaudited)")
        assert result == "income statement unaudited"
    
    def test_normalize_text_empty(self):
        result = normalize_text("")
        assert result == ""
    
    def test_normalize_text_none(self):
        result = normalize_text(None)
        assert result == ""
    
    def test_find_best_match_exact(self):
        candidates = ["income statement", "balance sheet", "cash flow"]
        result = find_best_match("income statement", candidates)
        assert result is not None
        match, score = result
        assert match == "income statement"
        assert score == 1.0
    
    def test_find_best_match_fuzzy(self):
        candidates = ["income statement", "balance sheet", "cash flow"]
        result = find_best_match("income stmt", candidates)
        assert result is not None
        match, score = result
        assert score >= 0.8
    
    def test_find_best_match_no_match(self):
        candidates = ["income statement", "balance sheet"]
        result = find_best_match("completely different", candidates, threshold=0.9)
        assert result is None
    
    def test_extract_xbrl_element_name(self):
        onclick = "top.Show.showAR( this, 'defref_us-gaap_CostOfGoodsSold'..."
        result = extract_xbrl_element_name(onclick)
        assert result == "us-gaap_CostOfGoodsSold"
    
    def test_extract_xbrl_element_name_no_match(self):
        onclick = "some other onclick text"
        result = extract_xbrl_element_name(onclick)
        assert result is None
    
    def test_extract_xbrl_element_name_empty(self):
        result = extract_xbrl_element_name("")
        assert result is None
    
    def test_extract_numeric_value_positive(self):
        result = extract_numeric_value("1,000,000")
        assert result == 1000000.0
    
    def test_extract_numeric_value_negative_parentheses(self):
        result = extract_numeric_value("(500,000)")
        assert result == -500000.0
    
    def test_extract_numeric_value_decimal(self):
        result = extract_numeric_value("1,234.56")
        assert result == 1234.56
    
    def test_extract_numeric_value_invalid(self):
        result = extract_numeric_value("not a number")
        assert result is None
    
    def test_is_numeric_cell_digits(self):
        assert is_numeric_cell("123") is True
        assert is_numeric_cell("1,234") is True
        assert is_numeric_cell("1,234.56") is True
    
    def test_is_numeric_cell_negative(self):
        assert is_numeric_cell("(123)") is True
        assert is_numeric_cell("(1,234.56)") is True
    
    def test_is_numeric_cell_text(self):
        assert is_numeric_cell("Revenue") is False
        assert is_numeric_cell("") is False
