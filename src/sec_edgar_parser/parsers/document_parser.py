from typing import Optional, Tuple, Dict, Any

from .dtd import DTD
from .document_text_parser import DocumentTextParser

class DocumentParser:
    """Parser for SEC EDGAR document structures"""
    
    def __init__(self):
        self.dtd = DTD()
    
    def parse(self, data: Dict[str, Any]) -> 'Document':
        """Parse document data into Document object"""
        return Document(data, self.dtd)


class Document:
    """Represents a parsed SEC EDGAR document"""
    
    def __init__(self, data: Dict[str, Any], dtd: DTD):
        self.dtd = dtd
        self.type = data.get(dtd.doc_type.tag)
        self.sequence = data.get(dtd.sequence.tag)
        self.filename = data.get(dtd.filename.tag)
        self.description = data.get(dtd.description.tag, None)
        
        doc_text_data = data.get(dtd.doc_text.tag)
        if doc_text_data:
            self.doc_text = DocumentTextParser(doc_text_data)
        else:
            self.doc_text = None
    
    def get_issuer_trading_symbol(self) -> Tuple[Optional[str], Optional[str]]:
        """Extract CIK and trading symbol from XML content"""
        if not self.doc_text or not self.doc_text.xml:
            return None, None
        
        try:
            xml_soup = self.doc_text.xml
            cik_elem = xml_soup.find('issuercik')
            symbol_elem = xml_soup.find('issuertradingsymbol')
            
            if cik_elem and symbol_elem:
                cik = cik_elem.get_text().lstrip('0')
                symbol = symbol_elem.get_text()
                return cik, symbol
        except Exception:
            pass
        
        return None, None
    
    def get_document_info(self) -> Dict[str, Any]:
        """Get basic document information"""
        return {
            'type': self.type,
            'sequence': self.sequence,
            'filename': self.filename,
            'description': self.description,
            'has_xml': self.doc_text.xml is not None if self.doc_text else False,
            'has_pdf': self.doc_text.pdf is not None if self.doc_text else False,
            'has_xbrl': self.doc_text.xbrl is not None if self.doc_text else False
        }
