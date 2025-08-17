import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.sec_edgar_parser.parsers import (
    SgmlParser,
    DTD,
    DtdElement,
    DocumentParser,
    Document,
    DocumentTextParser,
    FinancialStatementParser,
)
from src.sec_edgar_parser.core.exceptions import ParsingException


class TestDtdElement:
    def test_dtd_element_creation(self):
        element = DtdElement('<TEST>', True, False, True, None)
        assert element.tag == '<TEST>'
        assert element.has_end_tag is True
        assert element.repeats is False
        assert element.required is True
        assert element.parent is None
    
    def test_get_end_tag_string(self):
        element = DtdElement('<TEST>', True, False, True, None)
        assert element.get_end_tag_string() == '</TEST>'


class TestDTD:
    def test_dtd_initialization(self):
        dtd = DTD()
        assert dtd.sec_document is not None
        assert dtd.document is not None
        assert dtd.doc_text is not None
    
    def test_dtd_map_creation(self):
        dtd = DTD()
        assert '<SEC-DOCUMENT>' in dtd.map
        assert '<DOCUMENT>' in dtd.map
        assert '<TEXT>' in dtd.map
    
    def test_get_all_children(self):
        dtd = DTD()
        children = dtd.get_all_children('<SEC-DOCUMENT>')
        assert '<SEC-HEADER>' in children
        assert '<DOCUMENT>' in children
    
    def test_get_element(self):
        dtd = DTD()
        element = dtd.get_element('<DOCUMENT>')
        assert element is not None
        assert element.tag == '<DOCUMENT>'
    
    def test_is_required(self):
        dtd = DTD()
        assert dtd.is_required('<DOCUMENT>') is True
        assert dtd.is_required('<DESCRIPTION>') is False


class TestSgmlParser:
    def test_sgml_parser_initialization(self):
        dtd = DTD()
        parser = SgmlParser(dtd)
        assert parser.dtd == dtd
    
    def test_get_next_tag(self):
        dtd = DTD()
        parser = SgmlParser(dtd)
        
        # Test with valid tag
        result = parser._get_next_tag('<TEST>content</TEST>')
        assert result == '<TEST>'
        
        # Test with no tag
        result = parser._get_next_tag('no tags here')
        assert result is None
    
    def test_contains_edgar_tags(self):
        dtd = DTD()
        parser = SgmlParser(dtd)
        
        children = ['<TEST1>', '<TEST2>']
        
        # Contains tags
        assert parser._contains_edgar_tags('<TEST1>content', children) is True
        
        # No tags
        assert parser._contains_edgar_tags('no tags', children) is False


class TestDocumentTextParser:
    def test_document_text_parser_dict_content(self):
        data = {
            '<XML>': '<xml>test</xml>',
            '<PDF>': 'pdf content',
            '<XBRL>': 'xbrl content'
        }
        
        parser = DocumentTextParser(data)
        assert parser.xml is not None
        assert parser.pdf == 'pdf content'
        assert parser.xbrl == 'xbrl content'
    
    def test_document_text_parser_string_content(self):
        data = '<XML><test>content</test></XML>'
        parser = DocumentTextParser(data)
        assert parser.xml is not None
    
    def test_has_content(self):
        parser = DocumentTextParser({})
        assert parser.has_content() is False
        
        parser.xml = 'test'
        assert parser.has_content() is True
    
    def test_get_content_summary(self):
        parser = DocumentTextParser({})
        summary = parser.get_content_summary()
        assert isinstance(summary, dict)
        assert 'xml' in summary
        assert 'pdf' in summary


class TestDocumentParser:
    def test_document_parser_initialization(self):
        parser = DocumentParser()
        assert parser.dtd is not None
    
    def test_parse_document(self):
        parser = DocumentParser()
        data = {
            '<TYPE>': '10-K',
            '<SEQUENCE>': '1',
            '<FILENAME>': 'test.txt',
            '<DESCRIPTION>': 'Test document',
            '<TEXT>': {}
        }
        
        doc = parser.parse(data)
        assert doc.type == '10-K'
        assert doc.sequence == '1'
        assert doc.filename == 'test.txt'
        assert doc.description == 'Test document'


class TestDocument:
    def test_document_creation(self):
        dtd = DTD()
        data = {
            '<TYPE>': '10-K',
            '<SEQUENCE>': '1',
            '<FILENAME>': 'test.txt',
            '<TEXT>': {}
        }
        
        doc = Document(data, dtd)
        assert doc.type == '10-K'
        assert doc.sequence == '1'
        assert doc.filename == 'test.txt'
    
    def test_get_document_info(self):
        dtd = DTD()
        data = {
            '<TYPE>': '10-K',
            '<SEQUENCE>': '1',
            '<FILENAME>': 'test.txt',
            '<TEXT>': {}
        }
        
        doc = Document(data, dtd)
        info = doc.get_document_info()
        
        assert info['type'] == '10-K'
        assert info['sequence'] == '1'
        assert info['filename'] == 'test.txt'


class TestFinancialStatementParser:
    def test_financial_statement_parser_initialization(self):
        parser = FinancialStatementParser()
        assert 'income' in parser.statement_patterns
        assert 'balance' in parser.statement_patterns
        assert 'cash_flow' in parser.statement_patterns
    
    def test_is_metric_label(self):
        parser = FinancialStatementParser()
        
        assert parser._is_metric_label('Revenue') is True
        assert parser._is_metric_label('Total Assets') is True
        assert parser._is_metric_label('Description') is False
        assert parser._is_metric_label('') is False
    
    def test_looks_like_date(self):
        parser = FinancialStatementParser()
        
        assert parser._looks_like_date('2023') is True
        assert parser._looks_like_date('12/31/2023') is True
        assert parser._looks_like_date('2023-12-31') is True
        assert parser._looks_like_date('December 31, 2023') is True
        assert parser._looks_like_date('Revenue') is False
