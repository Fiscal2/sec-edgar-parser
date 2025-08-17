import re
import logging
from typing import List, Optional, Tuple
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

def normalize_text(text: str) -> str:
    """Normalize text for consistent comparison"""
    if not text:
        return ""
    
    normalized = re.sub(r'\s+', ' ', text.lower().strip())
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    return normalized

def find_best_match(target: str, candidates: List[str], threshold: float = 0.8) -> Optional[Tuple[str, float]]:
    """Find the best matching candidate using fuzzy string matching"""
    if not candidates:
        return None
    
    target_normalized = normalize_text(target)
    best_match = None
    best_score = 0.0
    
    for candidate in candidates:
        candidate_normalized = normalize_text(candidate)
        score = SequenceMatcher(None, target_normalized, candidate_normalized).ratio()
        
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate
    
    return (best_match, best_score) if best_match else None

def extract_xbrl_element_name(onclick_text: str) -> Optional[str]:
    """Extract XBRL element name from onclick attribute"""
    if not onclick_text:
        return None
    
    # Pattern: onclick="top.Show.showAR( this, 'defref_us-gaap_CostOfGoodsSold'..."
    pattern = r"defref_([^']+)"
    match = re.search(pattern, onclick_text)
    
    if match:
        return match.group(1)
    
    return None

def extract_numeric_value(text: str) -> Optional[float]:
    """Extract numeric value from text, handling common financial formats"""
    if not text:
        return None
    
    # Remove common financial formatting
    cleaned = re.sub(r'[$,]', '', text.strip())
    
    # Handle negative numbers in parentheses
    is_negative = False
    if cleaned.startswith('(') and cleaned.endswith(')'):
        is_negative = True
        cleaned = cleaned[1:-1]

    number_pattern = r'^-?\d+(?:,\d{3})*(?:\.\d+)?$'
    if re.match(number_pattern, cleaned):
        try:
            value = float(cleaned.replace(',', ''))
            return -value if is_negative else value
        except ValueError:
            pass
    
    return None

def is_numeric_cell(cell_text: str) -> bool:
    """Check if a table cell contains numeric data"""
    if not cell_text:
        return False
    
    cleaned = cell_text.strip()
    
    # Check for common numeric indicators
    numeric_indicators = [
        r'^\d+$',  # Just digits
        r'^\d+\.\d+$',  # Decimal number
        r'^[\d,]+$',  # Digits with commas
        r'^[\d,]+\.\d+$',  # Decimal with commas
        r'^\(\d+\)$',  # Negative in parentheses
        r'^\(\d+\.\d+\)$',  # Negative decimal in parentheses
        r'^\([\d,]+\d+\)$',  # Negative with commas
        r'^\([\d,]+\d+\.\d+\)$',  # Negative decimal with commas
    ]
    
    for pattern in numeric_indicators:
        if re.match(pattern, cleaned):
            return True
    
    return False
