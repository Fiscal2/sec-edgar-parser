import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from ..core.models import Company, FinancialStatement
from ..core.exceptions import FilingNotFoundException
from ..utils.date_utils import format_date_for_display
from .stock_service import StockService
from .filing_service import FilingService
from .uploader_service import UploadService

logger = logging.getLogger(__name__)

class MainService:
    """Main service that orchestrates the entire SEC filing parsing process"""
    
    def __init__(self):
        self.stock_service = StockService()
        self.filing_service = FilingService()
        self.upload_service = UploadService()
    
    def process_company_filing(self, ticker: str, target_year: int) -> Dict[str, Any]:
        """
        Process a company filing for a specific year.
            
        Returns:
            Dictionary containing processing results
        """
        logger.info(f"Processing {ticker} 10-K targeting report year {target_year}")
        
        try:
            company = self.stock_service.get_company(ticker)
            logger.info(f"Found company: {company.name or company.symbol} (CIK: {company.cik})")
            
            for filing_year in (target_year, target_year + 1):
                try:
                    filing = self.stock_service.get_filing(ticker, 'annual', filing_year, 0)
                    
                    company_name = filing.extract_company_name()
                    listed_exchange = filing.extract_listed_exchanges()
                    
                    logger.info(f"Extracted company name: {company_name} and listed exchange {listed_exchange}")
                    
                    income, balance, cash, report_year = self._extract_financial_data(filing, ticker, target_year)
                    
                    if income and balance and cash:
                        logger.info(f"📦 Successfully parsed {ticker} 10-K with report year {report_year} (filed in {filing_year})")

                        try:
                            self.upload_service.upsert_financials(
                                ticker=ticker,
                                year=report_year,           # use the computed report year
                                quarter=0,                  # or 4 for annual; match your schema
                                income=income,
                                balance=balance,
                                cash=cash,
                                company_name=company_name,
                                listed_exchange=listed_exchange,
                            )
                            logger.info(f"⬆️ Uploaded {ticker} {report_year} to Supabase")
                        except Exception as e:
                            logger.warning(f"Upload to Supabase failed for {ticker} {report_year}: {e}")
                        
                        return {
                            'success': True,
                            'ticker': ticker,
                            'report_year': report_year,
                            'filing_year': filing_year,
                            'company_name': company_name,
                            'listed_exchange': listed_exchange,
                            'income_statement': income,
                            'balance_sheet': balance,
                            'cash_flow': cash,
                            'message': f"Successfully processed {ticker} for {report_year}"
                        }
                    else:
                        logger.info(f"Missing statements for {ticker} (filing year {filing_year}); trying next year...")
                        
                except FilingNotFoundException:
                    logger.warning(f"No 10-K found for {ticker} in {filing_year}")
                    continue
                except Exception as e:
                    logger.warning(f"Error processing {ticker} for {filing_year}: {e}")
                    continue
            
            error_msg = f"No 10-K with report year {target_year} found for {ticker}"
            logger.error(error_msg)
            
            return {
                'success': False,
                'ticker': ticker,
                'target_year': target_year,
                'error': error_msg,
                'message': error_msg
            }
            
        except Exception as e:
            error_msg = f"Failed to process {ticker} filing for {target_year}: {e}"
            logger.exception(error_msg)
            
            return {
                'success': False,
                'ticker': ticker,
                'target_year': target_year,
                'error': str(e),
                'message': error_msg
            }
    
    def _extract_financial_data(self, filing, ticker: str, target_year: int) -> Tuple[Optional[Dict], Optional[Dict], Optional[Dict], Optional[int]]:
        """
        Extract financial data from a filing, filtering by target year.
        
        Returns:
            Tuple of (income_data, balance_data, cash_data, report_year)
        """
        try:
            logger.info(f"Checking reports for {ticker} in year: {target_year}")
            logger.info(f"Getting financial statements from filing: {filing.url}")
            logger.info(f"Filing documents: {list(filing.documents.keys()) if hasattr(filing, 'documents') else 'No documents'}")
            
            income = filing.get_income_statements()
            balance = filing.get_balance_sheets()
            cash = filing.get_cash_flows()
            
            if not all([income, balance, cash]):
                logger.error(f"Missing financial statements for {ticker}")
                return None, None, None, None
            
            income_data = self._convert_old_financial_report(income, target_year)
            balance_data = self._convert_old_financial_report(balance, target_year)
            cash_data = self._convert_old_financial_report(cash, target_year)
            
            if income_data and balance_data and cash_data:
                report_year = max(
                    income_data.get('report_year', target_year),
                    balance_data.get('report_year', target_year),
                    cash_data.get('report_year', target_year)
                )
                
                logger.info(
                    f"Filtered data counts for year {target_year} — "
                    f"Income: {len(income_data.get('reports', []))}, "
                    f"Balance: {len(balance_data.get('reports', []))}, "
                    f"Cash: {len(cash_data.get('reports', []))}"
                )
                
                return income_data, balance_data, cash_data, report_year
            
            logger.error(f"No valid reports found for {ticker} in {target_year}")
            return None, None, None, None
            
        except Exception as e:
            logger.error(f"Error extracting financial data for {ticker} {target_year}: {e}")
            return None, None, None, None
    
    def _convert_old_financial_report(self, financial_report, target_year: int) -> Optional[Dict[str, Any]]:
        """Convert old FinancialReport object to legacy format for backward compatibility"""
        if not financial_report:
            return None
        try:
            if hasattr(financial_report, 'reports'):
                raw_reports = financial_report.reports
            elif hasattr(financial_report, 'data'):
                raw_reports = financial_report.data
            else:
                logger.warning(f"Unknown FinancialReport structure for {target_year}")
                return None
            
            if not raw_reports:
                return None
            
            # Convert FinancialInfo objects to plain dictionaries
            reports = []
            for item in raw_reports:
                if hasattr(item, '__dict__'):
                    item_dict = item.__dict__.copy()
                    for key, value in item_dict.items():
                        if hasattr(value, '__dict__'):
                            item_dict[key] = str(value)
                        elif not isinstance(value, (str, int, float, bool, type(None))):
                            item_dict[key] = str(value)
                    reports.append(item_dict)
                elif isinstance(item, dict):
                    if 'date' in item:
                        try:
                            item_date = datetime.strptime(item['date'], '%Y-%m-%d')
                            if item_date.year == target_year:
                                reports.append(item)
                        except (ValueError, TypeError):
                            reports.append(item)
                    else:
                        reports.append(item)
                else:
                    reports.append(str(item))
            
            if not reports:
                return None
            
            return {
                'company': getattr(financial_report, 'company', 'Unknown'),
                'date_filed': getattr(financial_report, 'date_filed', 'Unknown'),
                'reports': reports,
                'report_year': target_year
            }
            
        except Exception as e:
            logger.error(f"Error converting old FinancialReport: {e}")
            return None

    def _convert_to_legacy_format(self, statement: FinancialStatement, target_year: int) -> Optional[Dict[str, Any]]:
        """Convert modern FinancialStatement to legacy format for backward compatibility"""
        if not statement or not statement.periods:
            return None
        
        filtered_periods = []
        for period in statement.periods:
            if period.date.year == target_year:
                legacy_period = {
                    'date': format_date_for_display(period.date),
                    'months': period.months,
                    'map': {}
                }
                
                for metric_name, metric in period.metrics.items():
                    legacy_period['map'][metric_name] = {
                        'label': metric.label,
                        'value': metric.value
                    }
                
                filtered_periods.append(legacy_period)
        
        if not filtered_periods:
            return None
        
        return {
            'company': statement.company,
            'date_filed': format_date_for_display(statement.date_filed),
            'reports': filtered_periods,
            'report_year': target_year
        }
    
    def process_multiple_companies(self, tickers: List[str], years: List[int]) -> Dict[str, Any]:
        """
        Process multiple companies and years.
            
        Returns:
            Dictionary containing processing results for all companies
        """
        results = {
            'successful': [],
            'failed': [],
            'total_processed': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        for ticker in tickers:
            logger.info(f"\nStarting annual filings for {ticker}...")
            
            for year in years:
                results['total_processed'] += 1
                
                try:
                    result = self.process_company_filing(ticker, year)
                    
                    if result['success']:
                        results['successful'].append(f"{ticker} {year}")
                        logger.info(f"✅ Successfully processed {ticker} {year}")
                    else:
                        results['failed'].append(f"{ticker} {year}")
                        logger.error(f"❌ Failed to process {ticker} {year}")
                        
                except Exception as e:
                    error_msg = f"Unexpected error processing {ticker} {year}: {e}"
                    logger.exception(error_msg)
                    results['failed'].append(f"{ticker} {year}")
        
        results['end_time'] = datetime.now()
        results['duration'] = results['end_time'] - results['start_time']
        
        if results['total_processed'] > 0:
            results['success_rate'] = (len(results['successful']) / results['total_processed']) * 100
        else:
            results['success_rate'] = 0.0
        
        return results
    
    def close(self):
        """Close all service connections"""
        self.filing_service.close()

# Backward compatibility function
def process_company_filing(ticker: str, target_year: int) -> bool:
    """Backward compatibility function for processing company filings"""
    service = MainService()
    try:
        result = service.process_company_filing(ticker, target_year)
        return result['success']
    finally:
        service.close()
