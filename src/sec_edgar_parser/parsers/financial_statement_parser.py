import re
import logging
from typing import List, Optional
from bs4 import BeautifulSoup, Tag
from datetime import datetime

from ..core.models import FinancialStatement, FinancialPeriod, FinancialMetric
from ..core.exceptions import ParsingException
from ..utils.text_utils import extract_numeric_value
from ..utils.date_utils import parse_date_flexible

logger = logging.getLogger(__name__)

class FinancialStatementParser:
    """Parser for financial statements in SEC filings"""
    
    def __init__(self):
        self.statement_patterns = {
            'income': [
                r'consolidated\s+statements?\s+of\s+(?:income|operations|earnings)',
                r'income\s+statements?',
                r'statements?\s+of\s+(?:income|operations|earnings)',
                r'consolidated\s+statement\s+of\s+(?:income|operations|earnings)'
            ],
            'balance': [
                r'consolidated\s+balance\s+sheets?',
                r'consolidated\s+statements?\s+of\s+financial\s+position',
                r'balance\s+sheets?',
                r'statements?\s+of\s+financial\s+position'
            ],
            'cash_flow': [
                r'consolidated\s+statements?\s+of\s+cash\s+flows?',
                r'statements?\s+of\s+cash\s+flows?',
                r'cash\s+flows?\s+statements?'
            ]
        }
    
    def parse_statement(self, html_content: str, company: str, statement_type: str) -> Optional[FinancialStatement]:
        """Parse financial statement from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            statement_section = self._find_statement_section(soup, statement_type)
            if not statement_section:
                return None
            
            filing_date = self._extract_filing_date(soup)
            
            periods = self._parse_financial_data(statement_section, statement_type)
            
            if not periods:
                return None
            
            return FinancialStatement(
                company=company,
                date_filed=filing_date,
                periods=periods
            )
            
        except Exception as e:
            logger.error(f"Error parsing {statement_type} statement for {company}: {e}")
            raise ParsingException(f"Failed to parse {statement_type} statement") from e
    
    def _find_statement_section(self, soup: BeautifulSoup, statement_type: str) -> Optional[Tag]:
        """Find the section containing the financial statement"""
        patterns = self.statement_patterns.get(statement_type, [])
        
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            
            for header in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                if regex.search(header.get_text()):
                    return self._get_statement_content(header)
            
            for table in soup.find_all('table'):
                caption = table.find('caption')
                if caption and regex.search(caption.get_text()):
                    return table
            
            for div in soup.find_all('div', class_=re.compile(r'(statement|report|financial)', re.IGNORECASE)):
                if regex.search(div.get_text()):
                    return div
        
        return None
    
    def _get_statement_content(self, header: Tag) -> Tag:
        """Get the content following a header"""
        current = header.find_next_sibling()
        while current:
            if current.name in ['table', 'div']:
                return current
            current = current.find_next_sibling()
        
        return header.parent
    
    def _extract_filing_date(self, soup: BeautifulSoup) -> datetime:
        """Extract filing date from document"""
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
                parsed_date = parse_date_flexible(date_text)
                if parsed_date:
                    return parsed_date
        
        return datetime.now()
    
    def _parse_financial_data(self, section: Tag, statement_type: str) -> List[FinancialPeriod]:
        """Parse financial data from statement section"""
        periods = []
        
        tables = section.find_all('table')
        if not tables:
            tables = [section] if section.name == 'table' else []
        
        for table in tables:
            table_periods = self._parse_table(table, statement_type)
            if table_periods:
                periods.extend(table_periods)
        
        return periods
    
    def _parse_table(self, table: Tag, statement_type: str) -> List[FinancialPeriod]:
        """Parse financial data from a table"""
        periods = []
        
        header_row = self._find_header_row(table)
        if not header_row:
            return periods
        
        dates = self._extract_dates_from_header(header_row)
        if not dates:
            return periods
        
        data_rows = self._get_data_rows(table, header_row)
        for row in data_rows:
            period = self._parse_data_row(row, dates, statement_type)
            if period:
                periods.append(period)
        
        return periods
    
    def _find_header_row(self, table: Tag) -> Optional[Tag]:
        """Find the header row containing dates"""
        rows = table.find_all('tr')
        if not rows:
            return None
        
        for row in rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) < 2:
                continue
            
            first_cell = cells[0].get_text().strip()
            if self._is_metric_label(first_cell):
                date_cells = cells[1:]
                if any(self._looks_like_date(cell.get_text()) for cell in date_cells):
                    return row
        
        return rows[0] if rows else None
    
    def _is_metric_label(self, text: str) -> bool:
        """Check if text looks like a financial metric label"""
        if not text:
            return False
        
        metric_indicators = [
            'revenue', 'income', 'expense', 'asset', 'liability',
            'equity', 'cash', 'debt', 'earnings', 'profit', 'loss',
            'sales', 'cost', 'margin', 'ratio', 'per share'
        ]
        
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in metric_indicators)
    
    def _looks_like_date(self, text: str) -> bool:
        """Check if text looks like a date"""
        if not text:
            return False
        
        date_patterns = [
            r'\d{4}',  # Year
            r'\d{1,2}/\d{1,2}/\d{4}',  # MM/DD/YYYY
            r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD
            r'[A-Za-z]+\s+\d{1,2},\s+\d{4}'  # Month DD, YYYY
        ]
        
        return any(re.search(pattern, text) for pattern in date_patterns)
    
    def _extract_dates_from_header(self, header_row: Tag) -> List[datetime]:
        """Extract dates from header row"""
        dates = []
        cells = header_row.find_all(['th', 'td'])
        
        for cell in cells[1:]:
            date_text = cell.get_text().strip()
            if date_text and not self._is_metric_label(date_text):
                parsed_date = parse_date_flexible(date_text)
                if parsed_date:
                    dates.append(parsed_date)
        
        return dates
    
    def _get_data_rows(self, table: Tag, header_row: Tag) -> List[Tag]:
        """Get data rows after header row"""
        rows = table.find_all('tr')
        if not rows:
            return []
        
        try:
            header_index = rows.index(header_row)
            return rows[header_index + 1:]
        except ValueError:
            return rows[1:] if len(rows) > 1 else []
    
    def _parse_data_row(self, row: Tag, dates: List[datetime], statement_type: str) -> Optional[FinancialPeriod]:
        """Parse a single data row"""
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
        
        months = 12 if statement_type in ['income', 'cash_flow'] else None
        
        return FinancialPeriod(
            date=dates[0] if dates else datetime.now(),
            months=months,
            metrics=metrics
        )
    
    def _extract_metric_name(self, cell: Tag) -> Optional[str]:
        """Extract metric name from cell"""
        text = cell.get_text().strip()
        if text and not text.lower().startswith(('description', 'item', '')):
            return text
        
        link = cell.find('a')
        if link and link.get('onclick'):
            onclick = link.get('onclick', '')
            match = re.search(r"defref_([^']+)", onclick)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_cell_value(self, cell: Tag) -> Optional[float]:
        """Extract numeric value from cell"""
        text = cell.get_text().strip()
        if not text:
            return None
        
        return extract_numeric_value(text)
    
    def _extract_namespace(self, cell: Tag) -> Optional[str]:
        """Extract XBRL namespace from cell"""
        link = cell.find('a')
        if link and link.get('onclick'):
            onclick = link.get('onclick', '')
            match = re.search(r"defref_([^']+)", onclick)
            if match and '_' in match.group(1):
                return match.group(1).split('_')[0]
        
        return None
