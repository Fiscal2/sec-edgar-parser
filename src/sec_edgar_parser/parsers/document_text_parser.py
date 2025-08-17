from typing import Optional, Dict, Any
from bs4 import BeautifulSoup

class DocumentTextParser:
    """Parser for document text content (XML, PDF, XBRL, etc.)"""
    
    def __init__(self, data: Any):
        self._parse_content(data)
    
    def _parse_content(self, data: Any):
        """Parse different content types"""
        if isinstance(data, dict):
            self._parse_dict_content(data)
        elif isinstance(data, str):
            self._parse_string_content(data)
        else:
            self._set_defaults()
    
    def _parse_dict_content(self, data: Dict[str, Any]):
        """Parse content from dictionary structure"""
        self.xml = self._parse_xml(data.get('<XML>'))
        self.pdf = data.get('<PDF>')
        self.xbrl = data.get('<XBRL>')
        self.table = data.get('<TABLE>')
        self.caption = data.get('<CAPTION>')
        self.stub = data.get('<S>')
        self.column = data.get('<C>')
        self.footnotes = data.get('<FN>')
    
    def _parse_string_content(self, data: str):
        """Parse content from string"""
        self._set_defaults()
        if '<XML>' in data:
            self.xml = self._parse_xml(data)
    
    def _parse_xml(self, xml_data: Any) -> Optional[BeautifulSoup]:
        """Parse XML content into BeautifulSoup object"""
        if not xml_data:
            return None
        
        try:
            if isinstance(xml_data, str):
                return BeautifulSoup(xml_data, 'xml')
            elif isinstance(xml_data, BeautifulSoup):
                return xml_data
            else:
                return None
        except Exception:
            return None
    
    def _set_defaults(self):
        """Set default values for content"""
        self.xml = None
        self.pdf = None
        self.xbrl = None
        self.table = None
        self.caption = None
        self.stub = None
        self.column = None
        self.footnotes = None
    
    def get_xml_content(self) -> Optional[str]:
        """Get XML content as string"""
        return str(self.xml) if self.xml else None
    
    def has_content(self) -> bool:
        """Check if document has any content"""
        return any([
            self.xml, self.pdf, self.xbrl, 
            self.table, self.caption, self.stub, 
            self.column, self.footnotes
        ])
    
    def get_content_summary(self) -> Dict[str, bool]:
        """Get summary of available content types"""
        return {
            'xml': self.xml is not None,
            'pdf': self.pdf is not None,
            'xbrl': self.xbrl is not None,
            'table': self.table is not None,
            'caption': self.caption is not None,
            'stub': self.stub is not None,
            'column': self.column is not None,
            'footnotes': self.footnotes is not None
        }
