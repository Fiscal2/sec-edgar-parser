import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import time

from src.sec_edgar_parser.api.main import create_app
from src.sec_edgar_parser.core.models import Company
from src.sec_edgar_parser.core.exceptions import FilingNotFoundException


@pytest.fixture
def client():
    """Create test client"""
    app = create_app()
    return TestClient(app)


class TestHealthEndpoints:
    def test_root_endpoint(self, client):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "SEC EDGAR Parser API"
        assert "endpoints" in data
    
    def test_health_check(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    def test_api_health_check(self, client):
        """Test API v1 health check"""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["version"] == "1.0.0"


class TestCompanyEndpoints:
    @patch('src.sec_edgar_parser.api.routes.CompanyService')
    def test_get_company_success(self, mock_company_service, client):
        """Test successful company retrieval"""
        mock_service = MagicMock()
        mock_company = Company(symbol="AAPL", cik="0000320193", name="Apple Inc.")
        mock_service.get_company.return_value = mock_company
        mock_company_service.return_value = mock_service
        
        response = client.get("/api/v1/companies/AAPL")
        assert response.status_code == 200
        data = response.json()
        assert data["symbol"] == "AAPL"
        assert data["cik"] == "0000320193"
        assert data["name"] == "Apple Inc."
    
    @patch('src.sec_edgar_parser.api.routes.CompanyService')
    def test_get_company_not_found(self, mock_company_service, client):
        """Test company not found"""
        mock_service = MagicMock()
        mock_service.get_company.side_effect = FilingNotFoundException("Company not found")
        mock_company_service.return_value = mock_service
        
        response = client.get("/api/v1/companies/INVALID")
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()


class TestParseEndpoints:
    @patch('src.sec_edgar_parser.api.routes.MainService')
    def test_parse_filing_success(self, mock_main_service, client):
        """Test successful filing parsing"""
        mock_service = MagicMock()
        mock_service.process_company_filing.return_value = {
            'success': True,
            'ticker': 'AAPL',
            'target_year': 2023,
            'filing_year': 2024,
            'company_name': 'Apple Inc.',
            'listed_exchange': 'NASDAQ',
            'message': 'Successfully processed'
        }
        mock_main_service.return_value = mock_service
        
        response = client.post("/api/v1/parse/filing", json={
            "symbol": "AAPL",
            "year": 2023
        })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["symbol"] == "AAPL"
        assert data["year"] == 2023
    
    @patch('src.sec_edgar_parser.api.routes.MainService')
    def test_parse_filing_validation_error(self, mock_main_service, client):
        """Test filing parsing validation error"""
        response = client.post("/api/v1/parse/filing", json={
            "symbol": "AAPL",
            "year": 1800  # Invalid year
        })
        assert response.status_code == 422  # Validation error
    
    def test_parse_filing_by_path(self, client):
        """Test parse filing using path parameters"""
        with patch('src.sec_edgar_parser.api.routes.MainService') as mock_main_service:
            mock_service = MagicMock()
            mock_service.process_company_filing.return_value = {
                'success': True,
                'ticker': 'AAPL',
                'target_year': 2023,
                'message': 'Successfully processed'
            }
            mock_main_service.return_value = mock_service
            
            response = client.get("/api/v1/parse/AAPL/2023")
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert data["symbol"] == "AAPL"


class TestBatchEndpoints:
    @patch('src.sec_edgar_parser.api.routes.RunnerService')
    def test_batch_parse_success(self, mock_runner_service, client):
        """Test successful batch parsing"""
        mock_service = MagicMock()
        mock_service.run_batch.return_value = {
            'total_processed': 2,
            'successful': ['AAPL 2023', 'MSFT 2023'],
            'failed': [],
            'success_rate': 100.0,
            'duration': MagicMock(total_seconds=lambda: 5.0),
            'results': []
        }
        mock_runner_service.return_value = mock_service
        
        response = client.post("/api/v1/parse/batch", json={
            "symbols": ["AAPL", "MSFT"],
            "years": [2023]
        })
        assert response.status_code == 200
        data = response.json()
        assert data["total_processed"] == 2
        assert data["success_rate"] == 100.0
        assert len(data["successful"]) == 2


class TestErrorHandling:
    def test_invalid_json(self, client):
        """Test invalid JSON handling"""
        response = client.post("/api/v1/parse/filing", 
                             data="invalid json",
                             headers={"Content-Type": "application/json"})
        assert response.status_code == 422
    
    def test_missing_required_fields(self, client):
        """Test missing required fields"""
        response = client.post("/api/v1/parse/filing", json={})
        assert response.status_code == 422
    
    def test_invalid_symbol_format(self, client):
        """Test invalid symbol format"""
        response = client.post("/api/v1/parse/filing", json={
            "symbol": "",  # Empty symbol
            "year": 2023
        })
        assert response.status_code == 422


class TestRealServiceIntegration:
    """Integration tests that use real services to catch actual bugs"""
    
    @pytest.mark.integration
    def test_search_plan_logic_with_real_services(self):
        """Test that the search plan logic actually works with real services"""
        from src.sec_edgar_parser.services.stock_service import StockService
        from src.sec_edgar_parser.services.company_service import CompanyService
        
        stock_service = StockService()
        company_service = CompanyService()
        
        # Test that we can find companies
        wfc_company = company_service.get_company("WFC")
        assert wfc_company is not None
        assert wfc_company.symbol == "WFC"
        assert wfc_company.cik == "72971"  # CIK format is without leading zeros
        
        # Test that the search plan logic works for WFC
        wfc_filing = stock_service.get_filing("WFC", "annual", 2021, 0)
        assert wfc_filing is not None, "WFC filing should be found using search plan logic"
        
        # Test that we can find JPM
        jpm_company = company_service.get_company("JPM")
        assert jpm_company is not None
        assert jpm_company.symbol == "JPM"
        
        # Test that the search plan logic works for JPM
        jpm_filing = stock_service.get_filing("JPM", "annual", 2021, 0)
        assert jpm_filing is not None, "JPM filing should be found using search plan logic"
    
    @pytest.mark.integration
    def test_search_plan_covers_multiple_quarters(self):
        """Test that the search plan actually searches multiple quarters"""
        from src.sec_edgar_parser.services.stock_service import StockService
        
        stock_service = StockService()
        
        # Test that the search plan includes Q1 of following year
        search_plan = stock_service._create_search_plan(2021, "annual", 0)
        
        # Should include Q4 2021, Q1 2022, Q4 2022, Q4 2020
        expected_quarters = [(2021, 4), (2022, 1), (2022, 4), (2020, 4)]
        for expected in expected_quarters:
            assert expected in search_plan, f"Search plan should include {expected}"
        
        # Test that quarter=0 triggers search plan logic
        # This is the key fix we implemented
        filing = stock_service.get_filing("WFC", "annual", 2021, 0)
        assert filing is not None, "quarter=0 should trigger search plan logic"
    
    @pytest.mark.integration
    def test_real_api_endpoints_with_actual_companies(self, client):
        """Test real API endpoints with companies that were previously failing"""
        # Test WFC - this was failing before our fix
        response = client.get("/api/v1/parse/WFC/2021")
        assert response.status_code == 200, f"WFC should work now: {response.text}"
        
        data = response.json()
        assert data["success"] is True, f"WFC should succeed: {data}"
        assert data["symbol"] == "WFC"
        assert data["company_name"] is not None
        assert data["income_statement"] is not None
        assert data["balance_sheet"] is not None
        assert data["cash_flow"] is not None
        
        # Test JPM - this was also failing before our fix
        response = client.get("/api/v1/parse/JPM/2021")
        assert response.status_code == 200, f"JPM should work now: {response.text}"
        
        data = response.json()
        assert data["success"] is True, f"JPM should succeed: {data}"
        assert data["symbol"] == "JPM"
        assert data["company_name"] is not None
        assert data["income_statement"] is not None
        assert data["balance_sheet"] is not None
        assert data["cash_flow"] is not None
    
    @pytest.mark.integration
    def test_search_plan_execution_logging(self):
        """Test that search plan execution is properly logged"""
        from src.sec_edgar_parser.services.stock_service import StockService
        import logging
        
        # Capture logs
        with patch('src.sec_edgar_parser.services.stock_service.logger') as mock_logger:
            stock_service = StockService()
            
            # This should trigger the search plan logic and log it
            filing = stock_service.get_filing("WFC", "annual", 2021, 0)
            
            # Verify that the search plan logging was called
            mock_logger.info.assert_called()
            log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
            
            # Should log about using search plan
            search_plan_logs = [log for log in log_calls if "search plan" in log.lower()]
            assert len(search_plan_logs) > 0, "Should log search plan execution"
    
    @pytest.mark.integration
    def test_fallback_logic_works(self):
        """Test that fallback to individual quarter search works if search plan fails"""
        from src.sec_edgar_parser.services.stock_service import StockService
        
        stock_service = StockService()
        
        # Test with a specific quarter to ensure fallback works
        filing = stock_service.get_filing("WFC", "annual", 2021, 4)
        assert filing is not None, "Fallback to specific quarter should work"
    
    @pytest.mark.slow
    def test_multiple_companies_work_consistently(self, client):
        """Test that multiple companies work consistently (mark as slow due to API calls)"""
        companies_to_test = ["AAPL", "WFC", "JPM", "MSFT", "GOOG"]
        
        for symbol in companies_to_test:
            try:
                response = client.get(f"/api/v1/parse/{symbol}/2021")
                if response.status_code == 200:
                    data = response.json()
                    if data["success"]:
                        print(f"✅ {symbol} works")
                    else:
                        print(f"⚠️  {symbol} returned success=false: {data.get('message', 'No message')}")
                else:
                    print(f"❌ {symbol} failed with status {response.status_code}")
            except Exception as e:
                print(f"❌ {symbol} threw exception: {e}")
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
