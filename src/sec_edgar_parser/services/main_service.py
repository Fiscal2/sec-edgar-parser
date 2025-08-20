import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from ..core.models import FinancialStatement
from ..core.exceptions import FilingNotFoundException
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
        """Process a company's filing for a specific year"""
        try:
            # Try filing years: target_year and target_year + 1
            for filing_year in [target_year, target_year + 1]:
                try:
                    filing = self.stock_service.get_filing(ticker, 'annual', filing_year, 0)
                    company_name = filing.extract_company_name()
                    listed_exchange = filing.extract_listed_exchanges()
                    
                    income, balance, cash, report_year = self._extract_financial_data(filing, ticker, target_year)
                    
                    if income and balance and cash:
                        total_revenue = self._extract_total_revenue(income, report_year)
                       
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
                                total_revenue=total_revenue,
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
                            'total_revenue': total_revenue,
                            'message': f"Successfully processed {ticker} for {report_year}"
                        }
                        
                except FilingNotFoundException:
                    continue
                except Exception as e:
                    logger.warning(f"Error processing {ticker} for {filing_year}: {e}")
                    break
            
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
            income = filing.get_income_statements()
            balance = filing.get_balance_sheets()
            cash = filing.get_cash_flows()
            
            if not all([income, balance, cash]):
                logger.error(f"Missing financial statements for {ticker}")
                return None, None, None, None
            
            income_data = self._convert_to_legacy_format(income, target_year)
            balance_data = self._convert_to_legacy_format(balance, target_year)
            cash_data = self._convert_to_legacy_format(cash, target_year)
            
            if income_data and balance_data and cash_data:
                # Extract report_year from the first period
                report_year = target_year
                if income_data and len(income_data) > 0:
                    first_period = income_data[0]
                    if 'date' in first_period:
                        try:
                            # Parse DD-MM-YYYY format to get year
                            date_parts = first_period['date'].split('-')
                            if len(date_parts) == 3:
                                report_year = int(date_parts[2])
                        except (ValueError, IndexError):
                            report_year = target_year
                
                return income_data, balance_data, cash_data, report_year
            
            logger.error(f"No valid reports found for {ticker} in {target_year}")
            return None, None, None, None
            
        except Exception as e:
            logger.error(f"Error extracting financial data for {ticker} {target_year}: {e}")
            return None, None, None, None
    
    def _extract_total_revenue(self, income_data: Dict[str, Any], target_year: int) -> Optional[float]:
        """Extract total revenue from income statement data for a specific year"""
        if not income_data:
            return None
            
        # Handle both new array format and old dict format
        if isinstance(income_data, list):
            reports = income_data
        elif isinstance(income_data, dict) and 'reports' in income_data:
            reports = income_data.get('reports', [])
        else:
            return None
            
        if not reports:
            return None
            
        for report in reports:
            if not isinstance(report, dict):
                continue
                
            report_date = report.get('date', '')
            if not report_date:
                continue
                
            if str(target_year) in str(report_date):
                map_data = report.get('map', {})
                if not map_data:
                    continue
                
                # Case where map_data might be a string
                if isinstance(map_data, str):
                    try:
                        import ast
                        map_data = ast.literal_eval(map_data)
                    except (ValueError, SyntaxError):
                        revenue_labels = [
                            'total revenue', 'net sales', 'total net sales', 
                            'revenue', 'total revenues', 'consolidated revenue'
                        ]
                        for label in revenue_labels:
                            if label in map_data.lower():
                                import re
                                pattern = rf'{label}.*?(\d+(?:,\d+)*(?:\.\d+)?)'
                                match = re.search(pattern, map_data.lower())
                                if match:
                                    try:
                                        return float(match.group(1).replace(',', ''))
                                    except (ValueError, TypeError):
                                        continue
                        continue
                
                if not isinstance(map_data, dict):
                    continue
                    
                revenue_labels = [
                    'total revenue', 'net sales', 'total net sales', 
                    'revenue', 'total revenues', 'consolidated revenue'
                ]
                
                for label in revenue_labels:
                    if label in map_data:
                        revenue_item = map_data[label]
                        
                        # Case where revenue_item might be a string or dict
                        if isinstance(revenue_item, dict):
                            revenue_value = revenue_item.get('value')
                        elif isinstance(revenue_item, (str, int, float)):
                            revenue_value = revenue_item
                        else:
                            continue
                            
                        if revenue_value is not None:
                            try:
                                if isinstance(revenue_value, str):
                                    revenue_value = float(revenue_value.replace(',', ''))
                                return float(revenue_value)
                            except (ValueError, TypeError):
                                continue
                                
        return None

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
                logger.warning(f"No reports or data attribute found on financial_report")
                return None
            
            if not raw_reports:
                return None
            
            # First pass: filter reports by year using datetime objects
            filtered_reports = []
            for item in raw_reports:
                try:
                    if hasattr(item, '__dict__'):
                        # Handle object with __dict__
                        item_dict = item.__dict__.copy()
                        item_date = item_dict.get('date')
                        
                        # Check if this report is for the target year or next year
                        if isinstance(item_date, datetime):
                            if item_date.year in [target_year, target_year + 1]:
                                filtered_reports.append(item)
                        elif isinstance(item_date, str):
                            # Try to parse string date
                            try:
                                parsed_date = datetime.strptime(item_date, '%Y-%m-%d')
                                if parsed_date.year in [target_year, target_year + 1]:
                                    filtered_reports.append(item)
                            except ValueError:
                                # If parsing fails, check if year string is in the date
                                if str(target_year) in item_date or str(target_year + 1) in item_date:
                                    filtered_reports.append(item)
                        else:
                            # If no date or can't parse, include it to be safe
                            filtered_reports.append(item)
                            
                    elif isinstance(item, dict):
                        # Handle dictionary items
                        item_date = item.get('date')
                        
                        if item_date:
                            if isinstance(item_date, datetime):
                                if item_date.year in [target_year, target_year + 1]:
                                    filtered_reports.append(item)
                            elif isinstance(item_date, str):
                                try:
                                    parsed_date = datetime.strptime(item_date, '%Y-%m-%d')
                                    if parsed_date.year in [target_year, target_year + 1]:
                                        filtered_reports.append(item)
                                except ValueError:
                                    if str(target_year) in item_date or str(target_year + 1) in item_date:
                                        filtered_reports.append(item)
                        else:
                            # No date field, include it
                            filtered_reports.append(item)
                    else:
                        # Not a dict or object, include it
                        filtered_reports.append(item)
                except Exception as e:
                    logger.warning(f"Error processing item in _convert_old_financial_report: {e}")
                    continue
            
            if not filtered_reports:
                return None
            
            # Second pass: convert filtered reports to JSON-serializable format
            reports = []
            for item in filtered_reports:
                try:
                    if hasattr(item, '__dict__'):
                        item_dict = item.__dict__.copy()
                        for key, value in item_dict.items():
                            if hasattr(value, '__dict__'):
                                item_dict[key] = str(value)
                            elif isinstance(value, datetime):
                                item_dict[key] = value.isoformat()
                            elif not isinstance(value, (str, int, float, bool, type(None))):
                                item_dict[key] = str(value)
                        reports.append(item_dict)
                    elif isinstance(item, dict):
                        processed_item = {}
                        for key, value in item.items():
                            if isinstance(value, datetime):
                                processed_item[key] = value.isoformat()
                            else:
                                processed_item[key] = value
                        reports.append(processed_item)
                    else:
                        reports.append(str(item))
                except Exception as e:
                    logger.warning(f"Error converting item to JSON format: {e}")
                    continue
            
            # Get date_filed and convert to string if it's a datetime object
            date_filed = getattr(financial_report, 'date_filed', 'Unknown')
            if isinstance(date_filed, datetime):
                date_filed = date_filed.isoformat()
            
            result = {
                'date_filed': date_filed,
                'reports': reports,
            }
            
            return result
            
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
                # Format date as DD-MM-YYYY to match expected structure
                date_str = period.date.strftime('%d-%m-%Y')
                
                legacy_period = {
                    'date': date_str,
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
        
        # Return the structure that matches your exported format
        # The uploader service expects this to be JSON serializable
        return filtered_periods
    
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
