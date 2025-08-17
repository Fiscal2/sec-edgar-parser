import re
import logging
from typing import Dict, Any, Optional, List

from ..core.exceptions import ParsingException

logger = logging.getLogger(__name__)

class SgmlParser:
    """SGML parser for SEC EDGAR documents"""
    
    def __init__(self, dtd):
        self.dtd = dtd
        self._tag_cache = {}
    
    def parse(self, document: str) -> Dict[str, Any]:
        """Parse SGML document into structured data"""
        try:
            document = document.strip()
            return self._parse_recursive(document)
        except Exception as e:
            raise ParsingException(f"SGML parsing failed: {e}") from e
    
    def _parse_recursive(self, data: str) -> Dict[str, Any]:
        """Recursively parse SGML data"""
        result = {}
        
        while data.strip():
            tag = self._get_next_tag(data)
            if not tag:
                break
                
            if tag not in self.dtd.map:
                break
                
            element = self.dtd.map[tag]
            tag_start = data.find(tag)
            tag_end = tag_start + len(tag)
            
            if not element.has_end_tag:
                value, end = self._extract_until_next_tag(data, tag_end)
            else:
                value, end = self._extract_enclosed_data(data, tag_end, element)
            
            self._add_to_result(result, tag, value)
            data = data[end:].strip()
        
        return result
    
    def _extract_until_next_tag(self, data: str, tag_end: int) -> tuple[Any, int]:
        """Extract data until next tag for self-closing elements"""
        next_tag = self._get_next_tag(data[tag_end:])
        if next_tag:
            next_tag_start = data.find(next_tag)
            value = data[tag_end:next_tag_start].strip()
            return value, next_tag_start
        else:
            value = data[tag_end:].strip()
            return value, len(data)
    
    def _extract_enclosed_data(self, data: str, tag_end: int, element) -> tuple[Any, int]:
        """Extract data enclosed by start/end tags"""
        end_tag = element.get_end_tag_string()
        end_tag_start = data.find(end_tag)
        
        if end_tag_start == -1:
            return "", len(data)
        
        enclosed_data = data[tag_end:end_tag_start]
        children = self.dtd.get_all_children(element.tag)
        
        if self._contains_edgar_tags(enclosed_data, children):
            value = self._parse_recursive(enclosed_data)
        else:
            value = enclosed_data.strip()
        
        end = end_tag_start + len(end_tag)
        return value, end
    
    def _contains_edgar_tags(self, data: str, children: List[str]) -> bool:
        """Check if data contains any EDGAR tags"""
        return any(child in data for child in children)
    
    def _add_required_children(self, result: Dict[str, Any], children: List[str]):
        """Add required child elements with default values"""
        for child in children:
            child_element = self.dtd.map[child]
            if child_element.required:
                default_value = [] if child_element.repeats else ""
                result[child] = default_value
    
    def _add_to_result(self, result: Dict[str, Any], key: str, value: Any):
        """Add parsed value to result according to DTD rules"""
        if key is None:
            if isinstance(value, dict):
                result.update(value)
            return
        
        element = self.dtd.map[key]
        
        if element.repeats:
            if key not in result:
                result[key] = [value] if not isinstance(value, list) else value
            else:
                if isinstance(value, list):
                    result[key].extend(value)
                else:
                    result[key].append(value)
        else:
            result[key] = value
    
    def _get_next_tag(self, data: str) -> Optional[str]:
        """Get next opening tag from data"""
        if not data:
            return None
        
        match = re.search(r'<[^/][^>]*>', data)
        return match.group(0) if match else None


class SgmlException(ParsingException):
    """SGML parsing specific exception"""
    pass
