import logging
from datetime import datetime
from typing import Optional, Union
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)

def parse_date_flexible(date_string: Union[str, datetime, None]) -> Optional[datetime]:
    """Parse date from various formats with fallback strategies"""
    if not date_string:
        return None
    
    if isinstance(date_string, datetime):
        return date_string
    
    date_string = str(date_string).strip()
    
    # Common SEC date formats
    date_patterns = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%B %d, %Y',
        '%b %d, %Y',
        '%b. %d, %Y',
        '%d %b %Y',
        '%d %B %Y',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%SZ',
    ]
    
    for pattern in date_patterns:
        try:
            return datetime.strptime(date_string, pattern)
        except ValueError:
            continue
    
    try:
        return date_parser.parse(date_string)
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not parse date: {date_string}, error: {e}")
        return None

def format_date_for_display(date: Union[datetime, str, None], format_str: str = "%d-%m-%Y") -> Optional[str]:
    """Format date for display purposes"""
    if not date:
        return None
    
    parsed_date = parse_date_flexible(date)
    if not parsed_date:
        return None
    
    return parsed_date.strftime(format_str)

def is_valid_year(year: int) -> bool:
    """Check if year is within valid SEC EDGAR range"""
    return 1993 <= year <= 2100

def get_filing_year_range() -> tuple[int, int]:
    """Get the valid range of years for SEC filings"""
    return 1993, datetime.now().year + 1
