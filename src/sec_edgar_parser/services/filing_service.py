import logging
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup
import re

from ..core.models import FinancialStatement
from ..core.exceptions import ParsingException
from ..utils.http_utils import EdgarHttpClient
from .financial_parser_service import FinancialParserService

logger = logging.getLogger(__name__)

class FilingService:
    """Service for managing SEC filing operations"""
    
    def __init__(self):
        self.http_client = EdgarHttpClient()
        self.financial_parser = FinancialParserService()
    
    def get_filing_content(self, url: str) -> str:
        """Get the content of a filing from EDGAR"""
        try:
            return self.http_client.get_filing_content(url)
        except Exception as e:
            logger.error(f"Failed to fetch filing content from {url}: {e}")
            raise ParsingException(f"Could not fetch filing content: {e}") from e
    
    def parse_filing(self, url: str, company: str) -> Dict[str, Any]:
        """
        Parse a complete SEC filing and extract all available information.
        Returns: Dictionary containing parsed filing information
        """
        try:
            content = self.get_filing_content(url)
            
            filing_info = {
                'company': company,
                'url': url,
                'content': content,
                'financial_statements': {},
                'metadata': {}
            }
            
            company_name = self.extract_company_name(content)
            if company_name:
                filing_info['metadata']['company_name'] = company_name
            
            exchanges = self.extract_listed_exchanges(content)
            if exchanges:
                filing_info['metadata']['exchanges'] = exchanges
            
            form_type = self.extract_form_type(content)
            if form_type:
                filing_info['metadata']['form_type'] = form_type
            
            filing_info['financial_statements'] = self.parse_financial_statements(content, company)
            
            return filing_info
            
        except Exception as e:
            logger.error(f"Error parsing filing for {company}: {e}")
            raise ParsingException(f"Failed to parse filing for {company}") from e
    
    def extract_company_name(self, content: str) -> Optional[str]:
        """Extract company name from filing content"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            selectors = [
                'span.companyName',
                'div.companyName',
                'td.companyName',
                'h1.companyName',
                'title'
            ]
            
            for selector in selectors:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text().strip()
                    if text and len(text) > 3:
                        name = re.sub(r'\s+', ' ', text)
                        name = re.sub(r'[^\w\s\-&.,]', '', name)
                        if name:
                            return name
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting company name: {e}")
            return None
    
    def extract_listed_exchanges(self, content: str) -> Optional[str]:
        """Extract listed exchanges from filing content"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            exchange_patterns = [
                r'NASDAQ[:\s]*([A-Z]+)',
                r'NYSE[:\s]*([A-Z]+)',
                r'AMEX[:\s]*([A-Z]+)',
                r'listed\s+on\s+([A-Z\s]+)',
                r'exchange[:\s]*([A-Z\s]+)'
            ]
            
            text = soup.get_text()
            for pattern in exchange_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    exchange = match.group(1).strip()
                    if exchange:
                        return exchange
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting exchanges: {e}")
            return None
    
    def extract_form_type(self, content: str) -> Optional[str]:
        """Extract form type from filing content"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            form_patterns = [
                r'Form\s+([0-9A-Z/-]+)',
                r'([0-9A-Z/-]+)\s+Report',
                r'([0-9A-Z/-]+)\s+Filing'
            ]
            
            text = soup.get_text()
            for pattern in form_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    form_type = match.group(1).strip()
                    if form_type:
                        return form_type
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting form type: {e}")
            return None
    
    def parse_financial_statements(self, content: str, company: str) -> Dict[str, Optional[FinancialStatement]]:
        """Parse all financial statements from the filing content"""
        statements = {}
        
        try:
            income_statement = self.financial_parser.parse_financial_statement(
                content, company, 'income'
            )
            statements['income'] = income_statement
            
            balance_sheet = self.financial_parser.parse_financial_statement(
                content, company, 'balance'
            )
            statements['balance'] = balance_sheet
            
            cash_flow = self.financial_parser.parse_financial_statement(
                content, company, 'cash_flow'
            )
            statements['cash_flow'] = cash_flow
            
        except Exception as e:
            logger.warning(f"Error parsing financial statements for {company}: {e}")
        
        return statements
    
    def get_income_statement(self, content: str, company: str) -> Optional[FinancialStatement]:
        """Get income statement from filing content"""
        return self.financial_parser.parse_financial_statement(content, company, 'income')
    
    def get_balance_sheet(self, content: str, company: str) -> Optional[FinancialStatement]:
        """Get balance sheet from filing content"""
        return self.financial_parser.parse_financial_statement(content, company, 'balance')
    
    def get_cash_flow_statement(self, content: str, company: str) -> Optional[FinancialStatement]:
        """Get cash flow statement from filing content"""
        return self.financial_parser.parse_financial_statement(content, company, 'cash_flow')
    
    def prepare_for_parsing(self, content: str) -> str:
        """Prepare filing content for parsing (legacy compatibility)"""
        return content
    
    def close(self):
        """Close the HTTP client"""
        self.http_client.close()

# Backward compatibility class
class Filing:
    """Backward compatibility wrapper for the old Filing class"""
    
    def __init__(self, company: str, url: str):
        self.company = company
        self.url = url
        self.service = FilingService()
        self._content = None
        self._parsed_data = None
    
    @property
    def content(self) -> str:
        """Get the filing content"""
        if self._content is None:
            self._content = self.service.get_filing_content(self.url)
        return self._content
    
    def prepare_for_parsing(self) -> None:
        """Prepare the filing for parsing (legacy compatibility)"""
        # This is a no-op in the modern version
        pass
    
    def extract_company_name(self) -> Optional[str]:
        """Extract company name from the filing"""
        return self.service.extract_company_name(self.content)
    
    def extract_listed_exchanges(self) -> Optional[str]:
        """Extract listed exchanges from the filing"""
        return self.service.extract_listed_exchanges(self.content)
    
    def get_income_statements(self) -> Optional[FinancialStatement]:
        """Get income statement from the filing"""
        return self.service.get_income_statement(self.content, self.company)
    
    def get_balance_sheets(self) -> Optional[FinancialStatement]:
        """Get balance sheet from the filing"""
        return self.service.get_balance_sheet(self.content, self.company)
    
    def get_cash_flows(self) -> Optional[FinancialStatement]:
        """Get cash flow statement from the filing"""
        return self.service.get_cash_flow_statement(self.content, self.company)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.service.close()
