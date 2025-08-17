import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.sec_edgar_parser.services import (
    CompanyService,
    StockService,
    FinancialParserService,
    FilingService,
    MainService,
    RunnerService
)
from src.sec_edgar_parser.core.models import Company, FinancialStatement, FinancialPeriod, FinancialMetric
from src.sec_edgar_parser.core.exceptions import FilingNotFoundException, ValidationException
from src.sec_edgar_parser.utils.http_utils import EdgarHttpClient


class TestCompanyService:
    def test_company_service_initialization(self):
        service = CompanyService()
        assert service is not None
    
    @patch('pandas.read_csv')
    def test_get_company_success(self, mock_read_csv):
        # Mock pandas DataFrame
        mock_df = MagicMock()
        mock_df.loc.__getitem__.return_value = MagicMock()
        mock_df.loc.__getitem__.return_value.iloc.__getitem__.return_value = {
            'cik': '0000320193',
            'name': 'Apple Inc.'
        }
        mock_read_csv.return_value = mock_df
        
        service = CompanyService()
        company = service.get_company('AAPL')
        
        assert company.symbol == 'AAPL'
        assert company.cik == '0000320193'
        assert company.name == 'Apple Inc.'
    
    @patch('pandas.read_csv')
    def test_get_company_not_found(self, mock_read_csv):
        # Mock pandas DataFrame with no results
        mock_df = MagicMock()
        mock_df.loc.__getitem__.return_value = MagicMock()
        mock_df.loc.__getitem__.return_value.iloc.__getitem__.side_effect = IndexError()
        mock_read_csv.return_value = mock_df
        
        service = CompanyService()
        
        with pytest.raises(FilingNotFoundException):
            service.get_company('INVALID')


class TestStockService:
    def test_stock_service_initialization(self):
        service = StockService()
        assert service is not None
    
    @patch.object(CompanyService, 'get_company')
    def test_get_company(self, mock_get_company):
        mock_company = Company(symbol='AAPL', cik='0000320193')
        mock_get_company.return_value = mock_company
        
        service = StockService()
        company = service.get_company('AAPL')
        
        assert company.symbol == 'AAPL'
        assert company.cik == '0000320193'
    
    def test_validate_period_annual(self):
        service = StockService()
        
        # Valid annual period
        period = service._create_search_plan(2023, 'annual', 0)
        assert len(period) == 3
        assert (2023, 4) in period
        assert (2024, 4) in period
        assert (2022, 4) in period
    
    def test_validate_period_quarterly(self):
        service = StockService()
        
        # Valid quarterly period
        period = service._create_search_plan(2023, 'quarterly', 2)
        assert len(period) == 3
        assert (2023, 2) in period
        assert (2024, 2) in period
        assert (2022, 2) in period


class TestFinancialParserService:
    def test_financial_parser_service_initialization(self):
        service = FinancialParserService()
        assert service is not None
        assert 'income' in service.statement_names
        assert 'balance' in service.statement_names
        assert 'cash_flow' in service.statement_names
    
    @patch('bs4.BeautifulSoup')
    def test_parse_financial_statement_no_table(self, mock_soup):
        # Mock BeautifulSoup with no report table
        mock_soup_instance = Mock()
        mock_soup_instance.find.return_value = None
        mock_soup.return_value = mock_soup_instance
        
        service = FinancialParserService()
        result = service.parse_financial_statement('<html></html>', 'AAPL', 'income')
        
        assert result is None
    
    def test_extract_metric_name_with_text(self):
        service = FinancialParserService()
        
        # Mock BeautifulSoup element with text
        mock_cell = Mock()
        mock_cell.get_text.return_value = 'Revenue'
        
        result = service._extract_metric_name(mock_cell)
        assert result == 'Revenue'
    
    def test_extract_metric_name_with_xbrl(self):
        service = FinancialParserService()
        
        # Mock BeautifulSoup element with XBRL link
        mock_link = Mock()
        mock_link.get.return_value = "top.Show.showAR( this, 'defref_us-gaap_Revenue'..."
        
        mock_cell = Mock()
        mock_cell.get_text.return_value = ''
        mock_cell.find.return_value = mock_link
        
        result = service._extract_metric_name(mock_cell)
        assert result == 'us-gaap_Revenue'


class TestFilingService:
    @patch('src.sec_edgar_parser.services.filing_service.EdgarHttpClient')
    def test_filing_service_initialization(self, mock_http_client):
        mock_http_client.return_value = Mock()
        service = FilingService()
        assert service is not None
    
    @patch('src.sec_edgar_parser.services.filing_service.EdgarHttpClient')
    def test_get_filing_content(self, mock_http_client):
        mock_client_instance = Mock()
        mock_client_instance.get_filing_content.return_value = '<html>Test content</html>'
        mock_http_client.return_value = mock_client_instance
        
        service = FilingService()
        content = service.get_filing_content('http://example.com')
        
        assert content == '<html>Test content</html>'
    
    @patch('src.sec_edgar_parser.services.filing_service.EdgarHttpClient')
    def test_extract_company_name_success(self, mock_http_client):
        mock_http_client.return_value = Mock()
        service = FilingService()
        
        # Mock HTML content with company name
        html_content = '''
        <html>
            <span class="companyName">Apple Inc.</span>
        </html>
        '''
        
        result = service.extract_company_name(html_content)
        assert result == 'Apple Inc.'
    
    @patch('src.sec_edgar_parser.services.filing_service.EdgarHttpClient')
    def test_extract_form_type_success(self, mock_http_client):
        mock_http_client.return_value = Mock()
        service = FilingService()
        
        # Mock HTML content with form type
        html_content = '''
        <html>
            <div>Form 10-K Report</div>
        </html>
        '''
        
        result = service.extract_form_type(html_content)
        assert result == '10-K'


class TestMainService:
    @patch('src.sec_edgar_parser.services.main_service.FilingService')
    @patch('src.sec_edgar_parser.services.main_service.StockService')
    def test_main_service_initialization(self, mock_stock_service, mock_filing_service):
        mock_stock_service.return_value = Mock()
        mock_filing_service.return_value = Mock()
        service = MainService()
        assert service is not None
    
    @patch('src.sec_edgar_parser.services.main_service.FilingService')
    @patch('src.sec_edgar_parser.services.main_service.StockService')
    def test_process_company_filing_company_not_found(self, mock_stock_service, mock_filing_service):
        mock_stock_service.return_value = Mock()
        mock_filing_service.return_value = Mock()
        
        mock_stock_instance = Mock()
        mock_stock_instance.get_company.side_effect = FilingNotFoundException("Company not found")
        mock_stock_service.return_value = mock_stock_instance
        
        service = MainService()
        result = service.process_company_filing('INVALID', 2023)
        
        assert result['success'] is False
        assert 'Company not found' in result['error']
    
    @patch('src.sec_edgar_parser.services.main_service.FilingService')
    @patch('src.sec_edgar_parser.services.main_service.StockService')
    def test_convert_to_legacy_format_success(self, mock_stock_service, mock_filing_service):
        mock_stock_service.return_value = Mock()
        mock_filing_service.return_value = Mock()
        service = MainService()
        
        # Create a mock FinancialStatement
        mock_metric = FinancialMetric(label='Revenue', value=1000000)
        mock_period = FinancialPeriod(
            date=datetime(2023, 12, 31),
            months=12,
            metrics={'revenue': mock_metric}
        )
        mock_statement = FinancialStatement(
            company='AAPL',
            date_filed=datetime(2024, 1, 27),
            periods=[mock_period]
        )
        
        result = service._convert_to_legacy_format(mock_statement, 2023)
        
        assert result is not None
        assert result['company'] == 'AAPL'
        assert len(result['reports']) == 1
        assert result['reports'][0]['map']['revenue']['label'] == 'Revenue'
        assert result['reports'][0]['map']['revenue']['value'] == 1000000


class TestRunnerService:
    @patch('src.sec_edgar_parser.services.runner_service.MainService')
    def test_runner_service_initialization(self, mock_main_service):
        mock_main_service.return_value = Mock()
        service = RunnerService()
        assert service is not None
    
    @patch('src.sec_edgar_parser.services.runner_service.MainService')
    def test_run_multiple_success(self, mock_main_service):
        mock_main_service.return_value = Mock()
        
        mock_main_instance = Mock()
        mock_main_instance.process_company_filing.return_value = {
            'success': True,
            'message': 'Success',
            'error': None
        }
        mock_main_service.return_value = mock_main_instance
        
        service = RunnerService()
        results = service.run_multiple('AAPL', [2023])
        
        assert len(results) == 1
        assert results[0]['year'] == 2023
        assert results[0]['success'] is True
        assert results[0]['message'] == 'Success'
    
    @patch('src.sec_edgar_parser.services.runner_service.MainService')
    def test_run_multiple_failure(self, mock_main_service):
        mock_main_service.return_value = Mock()
        
        mock_main_instance = Mock()
        mock_main_instance.process_company_filing.return_value = {
            'success': False,
            'message': 'Failed',
            'error': 'Test error'
        }
        mock_main_service.return_value = mock_main_instance
        
        service = RunnerService()
        results = service.run_multiple('AAPL', [2023])
        
        assert len(results) == 1
        assert results[0]['year'] == 2023
        assert results[0]['success'] is False
        assert results[0]['error'] == 'Test error'
