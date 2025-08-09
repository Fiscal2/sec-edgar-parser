'''
Logic related to the handling of filings and documents
'''
# import csv
# import re
# import os
import difflib
import re
from bs4 import BeautifulSoup
import requests
from edgar.requests_wrapper import GetRequest
from edgar.document import Document
from edgar.sgml import Sgml
from edgar.dtd import DTD
from edgar.financials import get_financial_report
from datetime import datetime
import logging


FILING_SUMMARY_FILE = 'FilingSummary.xml'

logger = logging.getLogger(__name__)


class Statements:
    # used in parsing financial data; these are the statements we'll be parsing
    # To resolve "could not find anything for ShortName..." error, likely need
    # to add the appropriate ShortName from the FilingSummary.xml here.
    # TODO: perhaps add guessing/best match functionality to limit this list
    income_statements = ['consolidated statements of income',
                    'consolidated statement of income',
                    'consolidated statements of operations',
                    'income statements',
                    'consolidated statement of operations',                    
                    'consolidated statement of earnings',
                    'consolidated statements of earnings',    
                    'consolidated statements of operations and comprehensive income (loss)',
                    'consolidated statements of operations and comprehensive income',                
                    'condensed consolidated statements of income (unaudited)',
                    'condensed consolidated statements of income',
                    'condensed consolidated statements of operations (unaudited)',
                    'condensed consolidated statements of operations',
                    'condensed consolidated statement of earnings (unaudited)',
                    'condensed consolidated statement of earnings',
                    'condensed statements of income',
                    'condensed statements of operations',
                    'condensed statements of operations and comprehensive loss',
                    'consolidated statements of comprehensive income',
                    'statements of consolidated income'
                    ]
    balance_sheets = ['consolidated balance sheets',
                    'consolidated balance sheet',
                    'consolidated statement of financial position',
                    'consolidated statements of financial position',
                    'condensed consolidated statement of financial position (current period unaudited)',
                    'condensed consolidated statement of financial position (unaudited)',
                    'condensed consolidated statement of financial position',
                    'condensed consolidated balance sheets (current period unaudited)',
                    'condensed consolidated balance sheets (unaudited)',
                    'condensed consolidated balance sheets',
                    'condensed balance sheets',
                    'balance sheets',
                    ]
    cash_flows = ['consolidated statements of cash flows',
                    'consolidated statement of cash flows',
                    'consolidated statements of cash flows (unaudited)',
                    'condensed consolidated statements of cash flows (unaudited)',
                    'condensed consolidated statements of cash flows',
                    'condensed statements of cash flows',
                    'cash flows statements',
                    'statements of consolidated cash flows'
                    ]

    all_statements = income_statements + balance_sheets + cash_flows


class Filing:

    STATEMENTS = Statements()
    sgml = None


    def __init__(self, url, company=None):
        self.url = url
        # made this company instead of symbol since not all edgar companies are publicly traded
        self.company = company

        response = GetRequest(url).response
        text = response.text
        
        self.text = text

        print('Processing SGML at '+url)
        
        dtd = DTD()
        sgml = Sgml(text, dtd)

        self.sgml = sgml

        # {filename:Document}
        self.documents = {}
        for document_raw in sgml.map[dtd.sec_document.tag][dtd.document.tag]:
            document = Document(document_raw)
            self.documents[document.filename] = document
        
        acceptance_datetime_element = sgml.map[dtd.sec_document.tag][dtd.sec_header.tag][dtd.acceptance_datetime.tag]
        acceptance_datetime_text = acceptance_datetime_element[:8] # YYYYMMDDhhmmss, the rest is junk
        # not concerned with time/timezones
        self.date_filed = datetime.strptime(acceptance_datetime_text, '%Y%m%d')


    def get_financial_data(self):
        '''
        This is mostly just for easy QA to return all financial statements
        in a given file, but the intended workflow is for he user to pick
        the specific statement they want (income, balance, cash flows)
        '''
        return self._get_financial_data(self.STATEMENTS.all_statements, True)

    def _get_financial_data(self, statement_short_names, get_all):
        '''
        Returns financial data used for processing 10-Q and 10-K documents
        '''
        financial_data = []

        for names in self._get_statement(statement_short_names):
            short_name = names[0]
            filename = names[1]
            print('Getting financial data for {0} (filename: {1})'
                .format(short_name, filename))
            financial_html_text = self.documents[filename].doc_text.data

            financial_report = get_financial_report(self.company, self.date_filed, financial_html_text)

            if get_all:
                financial_data.append(financial_report)
            else:
                return financial_report

        return financial_data



    def _get_statement(self, statement_short_names):
        '''
        Return a list of tuples of (short_names, filenames) for
        statement_short_names in filing_summary_xml
        '''
        statement_names = []

        if FILING_SUMMARY_FILE in self.documents:
            filing_summary_doc = self.documents[FILING_SUMMARY_FILE]
            filing_summary_xml = filing_summary_doc.doc_text.xml

            for short_name in statement_short_names:
                if not short_name:
                    print("Skipping empty or None short_name in _get_statement")
                    continue

                filename = self.get_html_file_name(filing_summary_xml, short_name)
                if filename is not None:
                    statement_names.append((short_name, filename))
        else:
            print('No financial documents in this filing')

        if len(statement_names) == 0:
            print('No financial documents could be found. Likely need to \
            update constants in edgar.filing.Statements.')
            
        return statement_names



    @staticmethod
    def get_html_file_name(filing_summary_xml, report_short_name):
        def normalize(s: str) -> str:
            if not s:
                return ""
            s = s.lower()
            # remove anything in parentheses
            s = re.sub(r"\(.*?\)", "", s)
            # collapse whitespace
            return " ".join(s.split())

        target = normalize(report_short_name)

        # build list of (normalized_shortname, htmlfilename)
        candidates = []
        for rpt in filing_summary_xml.find_all("report"):
            sn = rpt.find("shortname")
            fn = rpt.find("htmlfilename")
            if not sn or not sn.get_text(strip=True) or not fn:
                continue
            norm = normalize(sn.get_text())
            candidates.append((norm, fn.get_text()))

        # exact match
        for norm, fn in candidates:
            if norm == target:
                return fn

        # Fuzzy fallback
        shortnames = [n for n, _ in candidates]
        best = difflib.get_close_matches(target, shortnames, n=1, cutoff=0.85)
        if best:
            return dict(candidates)[best[0]]

        print(f"could not find anything for ShortName {target!r}")
        return None
    
    def extract_company_name(self):
        try:
            logger.info(f"Fetching SGML from: {self.url}")
            res = GetRequest(self.url).response
            if res.status_code != 200:
                logger.error(f"Failed to fetch SGML: {res.status_code}")
                return None

            for line in res.text.splitlines():
                if "COMPANY CONFORMED NAME:" in line:
                    name = line.split("COMPANY CONFORMED NAME:")[1].strip()
                    logger.info(f"Extracted company name: {name}")
                    # Remove trailing "\XX" state code if present
                    name = re.sub(r'\\[A-Z]{2}\\?$', '', name)

                    return name
                

            logger.warning("COMPANY CONFORMED NAME not found.")
            return None
        except Exception as e:
            logger.error(f"Error extracting company name from SGML: {e}")
            return None
        
    def debug_print_shortnames(self):
        """Print all <shortName> values in the FilingSummary.xml for this filing."""
        if FILING_SUMMARY_FILE not in self.documents:
            print("No FilingSummary.xml found.")
            return

        xml = self.documents[FILING_SUMMARY_FILE].doc_text.xml
        print(f"\nAvailable ShortNames in {self.company or self.url}:")
        for rpt in xml.find_all("report"):
            sn = rpt.find("shortname")
            if sn and sn.get_text(strip=True):
                print("  •", sn.get_text(strip=True))
        print()


    def get_income_statements(self):
        return self._get_financial_data(self.STATEMENTS.income_statements, False)

    def get_balance_sheets(self):
        return self._get_financial_data(self.STATEMENTS.balance_sheets, False)

    def get_cash_flows(self):
        return self._get_financial_data(self.STATEMENTS.cash_flows, False)