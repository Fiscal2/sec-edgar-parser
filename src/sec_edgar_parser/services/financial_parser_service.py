import logging
from datetime import datetime
from typing import List, Optional, Any
from bs4 import BeautifulSoup

from ..core.models import FinancialStatement, FinancialPeriod, FinancialMetric
from ..core.exceptions import ParsingException
from ..utils.text_utils import (
    find_best_match, 
    extract_xbrl_element_name, 
    extract_numeric_value,
    is_numeric_cell
)
from ..core.config import settings

logger = logging.getLogger(__name__)

class FinancialParserService:
    """Service for parsing financial statements from SEC filings"""
    
    def __init__(self):
        self.statement_names = {
            'income': settings.income_statement_names,
            'balance': settings.balance_sheet_names,
            'cash_flow': settings.cash_flow_names
        }
    
    def parse_financial_statement(self, html_content: str, company: str, statement_type: str) -> Optional[FinancialStatement]:
        """
        Parse a financial statement from HTML content.
        
        Args:
            html_content: HTML content of the financial statement
            company: Company symbol/name
            statement_type: Type of statement ('income', 'balance', 'cash_flow')
            
        Returns:
            Parsed FinancialStatement or None if parsing fails
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            table = soup.find('table', {'class': 'report'})
            if not table:
                logger.warning(f"No report table found for {company} {statement_type}")
                return None
            
            filing_date = self._extract_filing_date(soup)
            
            periods = self._parse_financial_table(table, statement_type)
            
            if not periods:
                logger.warning(f"No financial periods found for {company} {statement_type}")
                return None
            
            return FinancialStatement(
                company=company,
                date_filed=filing_date,
                periods=periods
            )
            
        except Exception as e:
            logger.error(f"Error parsing financial statement for {company}: {e}")
            raise ParsingException(f"Failed to parse {statement_type} statement for {company}") from e
    
    def _extract_filing_date(self, soup: BeautifulSoup) -> datetime:
        """Extract filing date from the document"""
        date_selectors = [
            'span.filingDate',
            'div.filingDate',
            'td.filingDate',
            'span[data-filing-date]',
            'div[data-filing-date]'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                date_text = element.get_text().strip()
                try:
                    from ..utils.date_utils import parse_date_flexible
                    parsed_date = parse_date_flexible(date_text)
                    if parsed_date:
                        return parsed_date
                except Exception:
                    continue
        
        logger.warning("Could not extract filing date, using current date")
        return datetime.now()
    
    def _parse_financial_table(self, table: BeautifulSoup, statement_type: str) -> List[FinancialPeriod]:
        """Parse financial data from the table"""
        periods = []
        
        rows = table.find_all('tr')
        if not rows:
            return periods
        
        header_row = rows[0]
        dates = self._extract_dates_from_header(header_row)
        
        for row in rows[1:]:
            period = self._parse_financial_row(row, dates, statement_type)
            if period:
                periods.append(period)
        
        return periods
    
    def _extract_dates_from_header(self, header_row: BeautifulSoup) -> List[datetime]:
        """Extract dates from the table header row"""
        dates = []
        cells = header_row.find_all(['th', 'td'])
        
        for cell in cells:
            date_text = cell.get_text().strip()
            if date_text and not date_text.lower().startswith(('description', 'item')):
                try:
                    from ..utils.date_utils import parse_date_flexible
                    parsed_date = parse_date_flexible(date_text)
                    if parsed_date:
                        dates.append(parsed_date)
                except Exception:
                    continue
        
        return dates
    
    def _parse_financial_row(self, row: BeautifulSoup, dates: List[datetime], statement_type: str) -> Optional[FinancialPeriod]:
        """Parse a single row of financial data"""
        cells = row.find_all(['td', 'th'])
        if len(cells) < 2:
            return None
        
        metric_cell = cells[0]
        metric_name = self._extract_metric_name(metric_cell)
        if not metric_name:
            return None
        
        metrics = {}
        for i, date in enumerate(dates):
            if i + 1 < len(cells):
                value_cell = cells[i + 1]
                value = self._extract_cell_value(value_cell)
                if value is not None:
                    metric = FinancialMetric(
                        label=metric_name,
                        value=value,
                        namespace=self._extract_namespace(metric_cell)
                    )
                    metrics[metric_name] = metric
        
        if not metrics:
            return None
        
        months = None
        if statement_type in ['income', 'cash_flow']:
            months = 12
        
        period_date = dates[0] if dates else datetime.now()
        
        return FinancialPeriod(
            date=period_date,
            months=months,
            metrics=metrics
        )
    
    def _extract_metric_name(self, cell: BeautifulSoup) -> Optional[str]:
        """Extract metric name from a cell"""
        text = cell.get_text().strip()
        if text and text.lower() not in ['description', 'item', '']:
            return text
        
        link = cell.find('a')
        if link and link.get('onclick'):
            xbrl_name = extract_xbrl_element_name(link.get('onclick'))
            if xbrl_name:
                return xbrl_name
        
        return None
    
    def _extract_cell_value(self, cell: BeautifulSoup) -> Optional[float]:
        """Extract numeric value from a cell"""
        text = cell.get_text().strip()
        if not text:
            return None
        
        if is_numeric_cell(text):
            return extract_numeric_value(text)
        
        return None
    
    def _extract_namespace(self, cell: BeautifulSoup) -> Optional[str]:
        """Extract XBRL namespace from a cell"""
        link = cell.find('a')
        if link and link.get('onclick'):
            xbrl_name = extract_xbrl_element_name(link.get('onclick'))
            if xbrl_name and '_' in xbrl_name:
                return xbrl_name.split('_')[0]
        
        return None
    
    def find_statement_by_name(self, html_content: str, target_name: str) -> Optional[str]:
        """Find a financial statement by name using fuzzy matching"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        text_elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'span'])
        
        for element in text_elements:
            text = element.get_text().strip()
            if text:
                match = find_best_match(text, [target_name], threshold=0.7)
                if match:
                    logger.info(f"Found statement '{target_name}' with match '{match[0]}' (score: {match[1]:.2f})")
                    return text
        
        return None

# Backward compatibility functions
def get_financial_report(html_content: str, company: str, statement_type: str) -> Optional[FinancialStatement]:
    """Backward compatibility function for getting financial reports"""
    parser = FinancialParserService()
    return parser.parse_financial_statement(html_content, company, statement_type)

class FinancialReportEncoder:
    """Backward compatibility encoder for financial reports"""
    
    def encode(self, obj: Any) -> str:
        """Encode a financial report to JSON string"""
        import json
        return json.dumps(obj, default=self._default_encoder)
    
    def _default_encoder(self, obj: Any) -> Any:
        """Default encoder for datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return str(obj)
