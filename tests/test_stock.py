import pytest
import json
from src.sec_edgar_parser.services.stock_service import StockService
from src.sec_edgar_parser.services.company_service import CompanyService
from src.sec_edgar_parser.core.exceptions import FilingNotFoundException

    
def setup_module(module):
    print('setup_module      module:%s' % module.__name__)


def test_init():

    company_service = CompanyService()
    company = company_service.get_company('AAPL')
    assert company.symbol == 'AAPL'
    assert company.cik == '320193'

def test_get_filing():

    stock_service = StockService()
    filing = stock_service.get_filing('AAPL', 'quarterly', 2016, 1)
    assert filing is not None



############## Negative Testing ##############

def test_init_unknown_symbol():
    try:
        company_service = CompanyService()
        company_service.get_company('ZZZZZZZZZZZZZZZ')
        assert False
    except FilingNotFoundException:
        assert True

def test_get_filing_no_filing_found_exception():

    stock_service = StockService()
    try:
        # Try to get a filing for a completely non-existent company
        filing = stock_service.get_filing('NONEXISTENT123', 'quarterly', 2020, 1)
        assert False
    except FilingNotFoundException:
        assert True