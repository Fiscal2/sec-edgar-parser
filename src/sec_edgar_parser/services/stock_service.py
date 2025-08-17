import logging
from datetime import datetime
from typing import Optional, List, Tuple

from ..core.models import Company, FilingPeriod, FilingInfo
from ..core.exceptions import FilingNotFoundException, ValidationException
from ..utils.date_utils import is_valid_year
from .company_service import CompanyService

logger = logging.getLogger(__name__)

class StockService:
    """Service for managing stock-related operations"""
    
    def __init__(self):
        self.company_service = CompanyService()
    
    def get_company(self, symbol: str) -> Company:
        """Get company information by symbol"""
        return self.company_service.get_company(symbol)
    
    def get_filing(self, symbol: str, period: str = 'annual', year: int = 0, quarter: int = 0) -> 'Filing':
        """
        Get a filing for a company with search
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period: Filing period ('annual' or 'quarterly')
            year: Target year (0 for current year)
            quarter: Target quarter (0 for latest, 1-4 for specific)
        
        Returns:
            Filing object for the requested period
        """
        if period not in ['annual', 'quarterly']:
            raise ValidationException(f"Invalid period: {period}. Must be 'annual' or 'quarterly'")
        
        if not is_valid_year(year):
            raise ValidationException(f"Invalid year: {year}. Must be between 1993 and {datetime.now().year + 1}")
        
        if period == 'quarterly' and quarter == 0:
            raise ValidationException("Quarter must be specified for quarterly filings")
        
        company = self.get_company(symbol)
        target_year = datetime.now().year if year == 0 else year
        
        filing_period = FilingPeriod(
            type=period,
            year=target_year,
            quarter=quarter if period == 'quarterly' else None
        )
        
        search_plan = self._create_search_plan(target_year, period, quarter)
        
        try:
            filing_info = self._find_filing_info(company.cik, period, target_year, 0)
            if filing_info:
                logger.info(f"Found filing for {symbol} using search plan")
                return self._create_filing(company, filing_info)
        except Exception as e:
            logger.debug(f"Search plan failed for {symbol}: {e}")
        
        for y, q in search_plan:
            try:
                filing_info = self._find_filing_info(company.cik, period, y, q)
                if filing_info:
                    logger.info(f"Found filing for {symbol} in {y} Q{q}")
                    return self._create_filing(company, filing_info)
            except Exception as e:
                logger.debug(f"No filing found for {symbol} in {y} Q{q}: {e}")
                continue
        
        raise FilingNotFoundException(
            f'No filing found for {symbol} (period={period}, target_year={target_year})'
        )
    
    def _create_search_plan(self, target_year: int, period: str, quarter: int) -> List[Tuple[int, int]]:
        """Create search plan for finding filings"""
        search_plan = []
        
        if period == 'annual':
            search_plan += [(target_year, 4)]  # Same year Q4
            search_plan += [(target_year + 1, 1)]  # Next year Q1
            search_plan += [(target_year + 1, 4)]  # Next year Q4
            search_plan += [(target_year - 1, 4)]  # Prior year Q4
        else:
            search_plan += [(target_year, quarter)]  # Same year
            search_plan += [(target_year + 1, quarter)]  # Next year
            search_plan += [(target_year - 1, quarter)]  # Prior year
        
        return search_plan
    
    def _find_filing_info(self, cik: str, period: str, year: int, quarter: int) -> Optional[FilingInfo]:
        """Find filing info for a specific period"""
        # If quarter is 0, use search plan to search multiple quarters
        if quarter == 0:
            search_plan = self._create_search_plan(year, period, quarter)
            logger.info(f"Using search plan for {period} filings: {search_plan}")
            for search_year, search_quarter in search_plan:
                try:
                    from edgar.edgar import get_financial_filing_info
                    filing_info_list = get_financial_filing_info(period=period, cik=cik, year=search_year, quarter=search_quarter)
                    if filing_info_list:
                        info = filing_info_list[0]
                        return FilingInfo(
                            company=info.company,
                            form=info.form,
                            cik=info.cik,
                            date_filed=info.date_filed,
                            url=info.url,
                            file_path=info.url.split('/')[-1] if info.url else ""
                        )
                except Exception as e:
                    logger.debug(f"Legacy edgar module failed for {search_year} Q{search_quarter}: {e}")
                    continue
        else:
            try:
                from edgar.edgar import get_financial_filing_info
                filing_info_list = get_financial_filing_info(period=period, cik=cik, year=year, quarter=quarter)
                if filing_info_list:
                    info = filing_info_list[0]
                    return FilingInfo(
                        company=info.company,
                        form=info.form,
                        cik=info.cik,
                        date_filed=info.date_filed,
                        url=info.url,
                        file_path=info.url.split('/')[-1] if info.url else ""
                    )
            except Exception as e:
                logger.debug(f"Legacy edgar module failed: {e}")
        
        return None
    
    def _create_filing(self, company: Company, filing_info: FilingInfo) -> 'Filing':
        """Create a Filing object from filing info"""
        # Import here to avoid circular imports
        from edgar.filing import Filing
        return Filing(company=company.symbol, url=filing_info.url)
    
    def get_filings_for_period(self, symbol: str, start_year: int, end_year: int, period: str = 'annual') -> List['Filing']:
        """Get multiple filings for a range of years"""
        if not is_valid_year(start_year) or not is_valid_year(end_year):
            raise ValidationException(f"Invalid year range: {start_year}-{end_year}")
        
        if start_year > end_year:
            start_year, end_year = end_year, start_year
        
        filings = []
        for year in range(start_year, end_year + 1):
            try:
                filing = self.get_filing(symbol, period, year, 4 if period == 'annual' else 1)
                filings.append(filing)
            except FilingNotFoundException:
                logger.debug(f"No filing found for {symbol} in {year}")
                continue
        
        return filings

# Backward compatibility alias
Stock = StockService
