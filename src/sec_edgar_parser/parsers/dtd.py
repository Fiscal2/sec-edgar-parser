from typing import Optional, List

class DtdElement:
    """Represents a DTD element with its properties"""
    
    def __init__(self, tag: str, has_end_tag: bool, repeats: bool, required: bool, parent=None):
        self.tag = tag
        self.has_end_tag = has_end_tag
        self.repeats = repeats
        self.required = required
        self.parent = parent
    
    def get_end_tag_string(self) -> str:
        """Get the closing tag string"""
        return self.tag.replace('<', '</')
    
    def __repr__(self) -> str:
        parent_tag = self.parent.tag if self.parent else 'root'
        return f'<DtdElement [{self.tag}, {"end tag" if self.has_end_tag else "no end tag"}, {"repeats" if self.repeats else "not repeating"}, {"required" if self.required else "not required"}, {parent_tag}]>'


class DTD:
    """DTD for SEC EDGAR SGML documents"""
    
    def __init__(self):
        self._build_dtd()
    
    def _build_dtd(self):
        """Build the DTD structure"""
        self.sec_document = DtdElement('<SEC-DOCUMENT>', True, False, True, None)
        self.sec_header = DtdElement('<SEC-HEADER>', True, False, True, self.sec_document)
        self.acceptance_datetime = DtdElement('<ACCEPTANCE-DATETIME>', False, False, True, self.sec_header)
        
        self.document = DtdElement('<DOCUMENT>', True, True, True, self.sec_document)
        self.doc_type = DtdElement('<TYPE>', False, False, True, self.document)
        self.sequence = DtdElement('<SEQUENCE>', False, False, True, self.document)
        self.filename = DtdElement('<FILENAME>', False, False, True, self.document)
        self.description = DtdElement('<DESCRIPTION>', False, False, False, self.document)
        self.doc_text = DtdElement('<TEXT>', True, False, True, self.document)
        
        self.pdf = DtdElement('<PDF>', True, False, True, self.doc_text)
        self.xml = DtdElement('<XML>', True, False, True, self.doc_text)
        self.xbrl = DtdElement('<XBRL>', True, False, True, self.doc_text)
        self.table = DtdElement('<TABLE>', True, False, True, self.doc_text)
        self.caption = DtdElement('<CAPTION>', False, False, True, self.doc_text)
        self.stub = DtdElement('<S>', False, False, True, self.doc_text)
        self.column = DtdElement('<C>', False, False, True, self.doc_text)
        self.footnotes_section = DtdElement('<FN>', False, False, True, self.doc_text)
        
        self.element_list = [
            self.sec_document,
            self.sec_header,
            self.acceptance_datetime,
            self.document,
            self.doc_type,
            self.sequence,
            self.filename,
            self.description,
            self.doc_text,
            self.xml,  # Only XML for now, others commented out
        ]
        
        self.map = {element.tag: element for element in self.element_list}
    
    def get_all_children(self, root: Optional[str] = None) -> List[str]:
        """Get all children for a given root element"""
        if not root:
            return [tag for tag in self.map if self.map[tag].parent is None]
        
        return [tag for tag in self.map if self.map[tag].parent and self.map[tag].parent.tag == root]
    
    def get_element(self, tag: str) -> Optional[DtdElement]:
        """Get element by tag name"""
        return self.map.get(tag)
    
    def is_required(self, tag: str) -> bool:
        """Check if element is required"""
        element = self.map.get(tag)
        return element.required if element else False
    
    def repeats(self, tag: str) -> bool:
        """Check if element repeats"""
        element = self.map.get(tag)
        return element.repeats if element else False
