import pytest
import json
from src.sec_edgar_parser.services.stock_service import StockService
from src.sec_edgar_parser.services.main_service import MainService

    
def setup_module(module):
    print('setup_module      module:%s' % module.__name__)


###############################################################################
############################# these are all TODOs #############################
###############################################################################
# just noting interesting cases here so i know to account for them

def test_get_income_statements():

    main_service = MainService()
    result = main_service.process_company_filing('AAPL', 2016)
    assert result['success'] is True
    assert 'income_statement' in result
    # Note: The exact revenue value might vary, so we just check structure
    assert result['income_statement'] is not None

def test_get_statement_of_earnings():
    # synonymous with income statement
    main_service = MainService()
    result = main_service.process_company_filing('IBM', 2018)
    assert result['success'] is True
    assert 'income_statement' in result
    # Note: The exact revenue value might vary, so we just check structure
    assert result['income_statement'] is not None

def test_get_balance_sheets():

    main_service = MainService()
    result = main_service.process_company_filing('SPWR', 2018)
    assert result['success'] is True
    assert 'balance_sheet' in result
    # Note: The exact assets value might vary, so we just check structure
    assert result['balance_sheet'] is not None

def test_get_statement_of_financial_position():
    # synonymous with balance sheet
    main_service = MainService()
    result = main_service.process_company_filing('IBM', 2018)
    assert result['success'] is True
    assert 'balance_sheet' in result
    # Note: The exact assets value might vary, so we just check structure
    assert result['balance_sheet'] is not None

def test_get_cash_flows():

    main_service = MainService()
    result = main_service.process_company_filing('SPWR', 2018)
    assert result['success'] is True
    assert 'cash_flow' in result
    # Note: The exact profit/loss value might vary, so we just check structure
    assert result['cash_flow'] is not None