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
import gc
from lxml import etree


FILING_SUMMARY_FILE = 'FilingSummary.xml'

logger = logging.getLogger(__name__)


class Statements:
    # used in parsing financial data; these are the statements we'll be parsing
    # To resolve "could not find anything for ShortName..." error, likely need
    # to add the appropriate ShortName from the FilingSummary.xml here.
    # TODO: perhaps add guessing/best match functionality to limit this list
    income_statements = ['consolidated statements of income',
                    'consolidated statement of income',
                    'consolidated income statements',
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
                    'statements of consolidated income',
                    'consolidated statements of profit or loss and other comprehensive income',
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

        #print(f"Built {len(self.documents)} SGML documents (sample: {list(self.documents.keys())[:6]})")
        if FILING_SUMMARY_FILE in self.documents:
            print(f"FilingSummary.xml found: {self.documents[FILING_SUMMARY_FILE].doc_text.xml}")
        else:
            print(f"FilingSummary.xml NOT found in documents")
        self.text = None          # drop raw SGML text
        self.sgml = None          # drop parsed SGML tree
        gc.collect


    def get_financial_data(self):
        '''
        This is mostly just for easy QA to return all financial statements
        in a given file, but the intended workflow is for he user to pick
        the specific statement they want (income, balance, cash flows)
        '''
        return self._get_financial_data(self.STATEMENTS.all_statements, True)

    def _prune_documents(self, keep_files: set):
        """Keep only the target statement files (+ FilingSummary), drop the rest."""
        keep = set(keep_files) | {FILING_SUMMARY_FILE}
        removed = 0
        for k in list(self.documents.keys()):
            if k not in keep:
                self.documents.pop(k, None)
                removed += 1
        print(f"Pruned {removed} docs; kept {len(self.documents)}")

    def list_statement_files(self) -> set:
        """Return union of filenames we’ll need for income + balance + cash."""
        needed = set()
        for group in (
            self.STATEMENTS.income_statements,
            self.STATEMENTS.balance_sheets,
            self.STATEMENTS.cash_flows,
        ):
            for _, fn in self._get_statement(group):
                if fn:
                    needed.add(fn)
        return needed

    def prepare_for_parsing(self):
        """Discover all targets and prune once so we don't delete what we still need."""
        keep = self.list_statement_files()
        if keep:
            self._prune_documents(keep)
        else:
            print("No statement filenames discovered from FilingSummary; skipping prune.")


    def _get_financial_data(self, statement_short_names, get_all):
        """
        Returns financial data used for processing 10-Q and 10-K documents.
        Will skip statements that are missing or fail to parse, so the caller
        can retry with a different filing year.
        """
        financial_data = []

        for short_name, filename in self._get_statement(statement_short_names):
            #print(f'Getting financial data for {short_name} (filename: {filename})')

            # 1) Safe document lookup
            doc = self.documents.get(filename)
            if not doc:
                # Try case-insensitive match
                matches = [self.documents[k] for k in self.documents if k.lower() == filename.lower()]
                if matches:
                    doc = matches[0]
                else:
                    print(f"Skipping '{filename}' — not found in SGML documents")
                    continue

            # 2) Safe parsing
            try:
                financial_html_text = doc.doc_text.data
                financial_report = get_financial_report(self.company, self.date_filed, financial_html_text)
            except Exception as e:
                print(f"Failed to parse {filename}: {e}")
                continue

            # 3) Append or return based on `get_all`
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
        import re, difflib

        def normalize(s: str) -> str:
            if not s:
                return ""
            s = s.lower()
            s = re.sub(r"\(.*?\)", "", s)  # strip parentheticals
            s = " ".join(s.split())        # collapse whitespace
            return s

        def r_index(fn: str) -> int:
            # Pull numeric index from filenames like R3.htm / r12.htm
            m = re.search(r'[rR](\d+)\.htm', fn or "")
            return int(m.group(1)) if m else 9999

        target = normalize(report_short_name)

        # Build candidates
        candidates = []
        for rpt in filing_summary_xml.find_all(["report", "Report"]):
            sn = rpt.find(["shortname", "ShortName"])
            fn = (rpt.find(["htmlfilename", "HtmlFileName"]) or
                rpt.find(["filename", "FileName"]))
            if not sn or not sn.get_text(strip=True) or not fn:
                continue
            raw = sn.get_text(strip=True)
            norm = normalize(raw)
            fn_text = fn.get_text(strip=True)
            candidates.append({
                "raw": raw,
                "norm": norm,
                "fn": fn_text,
                "is_parenthetical": "parenthetical" in raw.lower(),
                "ridx": r_index(fn_text)
            })

        # Always prefer non-parenthetical
        non_paren = [c for c in candidates if not c["is_parenthetical"]]
        pool = non_paren if non_paren else candidates

        # 1) Exact match on normalized
        exact = [c for c in pool if c["norm"] == target]
        if exact:
            exact.sort(key=lambda c: (c["ridx"], len(c["norm"])))
            return exact[0]["fn"]

        # 2) Contains / contained-by (handles minor wording differences)
        loose = [c for c in pool if (target in c["norm"] or c["norm"] in target)]
        if loose:
            loose.sort(key=lambda c: (c["ridx"], abs(len(c["norm"]) - len(target))))
            return loose[0]["fn"]

        # 3) Fuzzy match with higher cutoff (be stricter)
        shortnames = [c["norm"] for c in pool]
        best = difflib.get_close_matches(target, shortnames, n=1, cutoff=0.9)
        if best:
            chosen = next(c for c in pool if c["norm"] == best[0])
            return chosen["fn"]

        # Last resort: fuzzy on all candidates with a slightly lower cutoff
        shortnames_all = [c["norm"] for c in candidates]
        best_any = difflib.get_close_matches(target, shortnames_all, n=1, cutoff=0.85)
        if best_any:
            chosen = next(c for c in candidates if c["norm"] == best_any[0])
            return chosen["fn"]

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

    def extract_listed_exchanges(self):
        """
        Extract only the Name of each exchange on which registered 
        (dei:SecurityExchangeName) from the filing.

        Returns:
            str or None: First exchange name found (as plain text), or None if not found
        """
        seen = set()

        for doc in self.documents.values():
            soup = getattr(doc.doc_text, "xml", None)
            if soup is None:
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(doc.doc_text.data, "lxml")
                except Exception:
                    continue

            for exch_tag in soup.find_all(attrs={"name": "dei:SecurityExchangeName"}):
                name = exch_tag.get_text(strip=True)
                if name and name not in seen:
                    return name  # Return immediately as a string

            try:
                del soup
            except Exception:
                pass

        return None  # If nothing found

    def get_income_statements(self):
        return self._get_financial_data(self.STATEMENTS.income_statements, False)

    def get_balance_sheets(self):
        return self._get_financial_data(self.STATEMENTS.balance_sheets, False)

    def get_cash_flows(self):
        return self._get_financial_data(self.STATEMENTS.cash_flows, False)