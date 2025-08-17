import pytest
from datetime import datetime
from decimal import Decimal

from src.sec_edgar_parser.core.models import (
    Company,
    FilingPeriod,
    FinancialMetric,
    FinancialPeriod,
    FinancialStatement,
)


class TestCompany:
    def test_company_creation(self):
        company = Company(symbol="AAPL", cik="0000320193")
        assert company.symbol == "AAPL"
        assert company.cik == "0000320193"
        assert company.name is None
    
    def test_company_with_name(self):
        company = Company(symbol="AAPL", cik="0000320193", name="Apple Inc.")
        assert company.name == "Apple Inc."
    
    def test_symbol_normalization(self):
        company = Company(symbol="  aapl  ", cik="0000320193")
        assert company.symbol == "AAPL"


class TestFilingPeriod:
    def test_annual_filing_period(self):
        period = FilingPeriod(type="annual", year=2023)
        assert period.type == "annual"
        assert period.year == 2023
        assert period.quarter is None
    
    def test_quarterly_filing_period(self):
        period = FilingPeriod(type="quarterly", year=2023, quarter=2)
        assert period.type == "quarterly"
        assert period.year == 2023
        assert period.quarter == 2
    
    def test_quarterly_requires_quarter(self):
        with pytest.raises(ValueError, match="Quarter must be specified"):
            FilingPeriod(type="quarterly", year=2023)


class TestFinancialMetric:
    def test_financial_metric_creation(self):
        metric = FinancialMetric(label="Revenue", value=1000000)
        assert metric.label == "Revenue"
        assert metric.value == Decimal("1000000")
    
    def test_financial_metric_with_namespace(self):
        metric = FinancialMetric(
            label="Cost of Goods Sold", 
            value=500000, 
            namespace="us-gaap"
        )
        assert metric.namespace == "us-gaap"
    
    def test_financial_metric_string_value(self):
        metric = FinancialMetric(label="Description", value="N/A")
        assert metric.value == "N/A"


class TestFinancialPeriod:
    def test_financial_period_creation(self):
        date = datetime(2023, 12, 31)
        metrics = {
            "revenue": FinancialMetric(label="Revenue", value=1000000),
            "cost": FinancialMetric(label="Cost", value=500000)
        }
        
        period = FinancialPeriod(date=date, months=12, metrics=metrics)
        assert period.date == date
        assert period.months == 12
        assert len(period.metrics) == 2


class TestFinancialStatement:
    def test_financial_statement_creation(self):
        company = Company(symbol="AAPL", cik="0000320193")
        date_filed = datetime(2024, 1, 27)
        
        statement = FinancialStatement(
            company=company.symbol,
            date_filed=date_filed,
            periods=[]
        )
        
        assert statement.company == "AAPL"
        assert statement.date_filed == date_filed
        assert len(statement.periods) == 0
    
    def test_add_period(self):
        statement = FinancialStatement(
            company="AAPL",
            date_filed=datetime(2024, 1, 27),
            periods=[]
        )
        
        period = FinancialPeriod(
            date=datetime(2023, 12, 31),
            months=12,
            metrics={}
        )
        
        statement.add_period(period)
        assert len(statement.periods) == 1
        assert statement.periods[0] == period
    
    def test_latest_period(self):
        statement = FinancialStatement(
            company="AAPL",
            date_filed=datetime(2024, 1, 27),
            periods=[]
        )
        
        # Add periods in reverse chronological order
        period1 = FinancialPeriod(
            date=datetime(2022, 12, 31),
            months=12,
            metrics={}
        )
        period2 = FinancialPeriod(
            date=datetime(2023, 12, 31),
            months=12,
            metrics={}
        )
        
        statement.add_period(period1)
        statement.add_period(period2)
        
        assert statement.latest_period == period2
