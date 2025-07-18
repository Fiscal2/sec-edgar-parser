'''
Logic related to the handling of filings and documents
'''
import re
from bs4 import BeautifulSoup
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
                    'consolidated statement of earnings',
                    'condensed consolidated statements of income (unaudited)',
                    'condensed consolidated statements of income',
                    'condensed consolidated statements of operations (unaudited)',
                    'condensed consolidated statements of operations',
                    'condensed consolidated statement of earnings (unaudited)',
                    'condensed consolidated statement of earnings',
                    'condensed statements of income',
                    'condensed statements of operations',
                    'condensed statements of operations and comprehensive loss',
                    ]
    balance_sheets = ['consolidated balance sheets',
                    'consolidated balance sheets',
                    'consolidated balance sheet',
                    'consolidated statement of financial position',
                    'condensed consolidated statement of financial position (current period unaudited)',
                    'condensed consolidated statement of financial position (unaudited)',
                    'condensed consolidated statement of financial position',
                    'condensed consolidated balance sheets (current period unaudited)',
                    'condensed consolidated balance sheets (unaudited)',
                    'condensed consolidated balance sheets',
                    'condensed balance sheets'
                    ]
    cash_flows = ['consolidated statements of cash flows',
                    'consolidated statement of cash flows',
                    'consolidated Statements of cash flows (unaudited)',
                    'condensed consolidated statements of cash flows (unaudited)',
                    'condensed consolidated statements of cash flows',
                    'condensed statements of cash flows'
                    ]

    revenue_breakdown_sections = [
        'disaggregated revenue',
        'consolidated results of operations',
        'revenue recognition',
        'revenue by source',
        'revenue by product',
        'revenue by segment',
        'revenue by geography',
        'revenue by customer type',
        'sales by product line',
        'net sales by segment',
        'revenues by business segment',
        'segment revenue',
        'product revenue',
        'service revenue',
        'revenue mix',
        'revenue composition',
        'segment reporting'
    ]

    all_statements = income_statements + balance_sheets + cash_flows

class RevenueBreakdownExtractor:
    """
    Enhanced class to extract revenue breakdown information from SEC filings
    Focuses on structured table data rather than text patterns
    """
    
    def __init__(self, filing_text, company=None):
        self.filing_text = filing_text
        self.company = company
        self.soup = BeautifulSoup(filing_text, 'lxml-xml')
    
    def extract_revenue_breakdown(self):
        """
        Extract revenue breakdown information from the filing
        Returns a structured dict with revenue categories and amounts
        """
        revenue_data = {
            'total_revenue': None,
            'revenue_breakdown': {},
            'revenue_sources': [],
            'extraction_method': None,
            'confidence_score': 0.0
        }
        
        # Try extraction methods in order of reliability
        methods = [
            self._extract_from_disaggregated_revenue_tables,
            self._extract_from_segment_tables,
            self._extract_from_merchandise_category_tables,
            self._extract_from_notes_tables
        ]
        
        for method in methods:
            try:
                result = method()
                if result and result['revenue_breakdown'] and len(result['revenue_breakdown']) > 0:
                    revenue_data = result
                    logger.info(f"Successfully extracted revenue data using {method.__name__}")
                    break
            except Exception as e:
                logger.warning(f"Error in revenue extraction method {method.__name__}: {e}")
                continue
        
        return revenue_data
    
    def _extract_from_disaggregated_revenue_tables(self):
        """
        Extract from tables with 'Disaggregated Revenue' headers
        """
        revenue_data = {
            'total_revenue': None,
            'revenue_breakdown': {},
            'revenue_sources': [],
            'extraction_method': 'disaggregated_revenue_tables',
            'confidence_score': 0.0
        }
        
        # Look for tables with disaggregated revenue
        tables = self.soup.find_all('table')
        
        for table in tables:
            table_text = table.get_text().lower()
            
            # Check if this table contains disaggregated revenue
            if 'disaggregated revenue' not in table_text:
                continue
            
            logger.info("Found disaggregated revenue table")
            
            # Extract revenue data from this table
            extracted_data = self._extract_revenue_from_table(table)
            if extracted_data:
                revenue_data.update(extracted_data)
                revenue_data['confidence_score'] = 0.95
                return revenue_data
        
        return revenue_data
    
    def _extract_from_segment_tables(self):
        """
        Extract from segment reporting tables
        """
        revenue_data = {
            'total_revenue': None,
            'revenue_breakdown': {},
            'revenue_sources': [],
            'extraction_method': 'segment_tables',
            'confidence_score': 0.0
        }
        
        tables = self.soup.find_all('table')
        
        for table in tables:
            table_text = table.get_text().lower().strip()
            logger.debug(f"Processing segment table: {table_text[:500]}...")
            
            # Broaden segment identification
            segment_keywords = [
                'segment', 'united states', 'canada', 'international', 
                'geographic', 'region', 'market', 'operations'
            ]
            if not any(keyword in table_text for keyword in segment_keywords):
                continue
            
            # Check for revenue-related terms
            revenue_keywords = ['revenue', 'sales', 'net sales', 'total revenue', 'income']
            if not any(keyword in table_text for keyword in revenue_keywords):
                continue
            
            logger.info("Found potential segment reporting table")
            
            # Extract data
            extracted_data = self._extract_segment_revenue_from_table(table)
            if extracted_data and extracted_data['revenue_breakdown']:
                revenue_data.update(extracted_data)
                revenue_data['confidence_score'] = 0.9
                return revenue_data
        
        return revenue_data
    
    def _extract_from_merchandise_category_tables(self):
        """
        Extract from merchandise category tables (e.g., Costco's Foods/Non-Foods, Tesla's Automotive/Energy).
        Aggregate data from all matching tables and set total revenue for any company.
        """
        revenue_data = {
            'total_revenue': None,
            'revenue_breakdown': {},
            'revenue_sources': [],
            'extraction_method': 'merchandise_category_tables',
            'confidence_score': 0.0
        }
        
        tables = self.soup.find_all('table')
        processed_tables = 0
        
        for table in tables:
            table_text = table.get_text().lower().strip()
            logger.debug(f"Processing merchandise table {processed_tables + 1}: {table_text[:1000]}...")
            
            # Broad revenue-related keywords for any industry
            merchandise_keywords = [
                'foods and sundries', 'non-foods', 'fresh foods', 'food', 'sundries',
                'ancillary', 'warehouse', 'grocery', 'apparel', 'electronics', 'pharmacy',
                'hardlines', 'softlines', 'membership fees', 'merchandise', 'ancillary and other',
                'net sales', 'total sales', 'automotive sales', 'automotive regulatory credits',
                'energy generation and storage', 'services and other', 'automotive leasing', 
                'energy generation and storage leasing', 'segment', 'product', 'service',
                'geographic', 'category', 'revenue', 'sales', 'disaggregated', 'iphone', 'mac', 'Wearables, Home and Accessories',
            ]
            
            if not any(keyword in table_text for keyword in merchandise_keywords):
                logger.debug(f"Table {processed_tables + 1} skipped: No matching keywords - {merchandise_keywords}")
                continue
            
            logger.info(f"Found merchandise category table {processed_tables + 1}")
            processed_tables += 1
            
            rows = table.find_all("tr")
            if not rows:
                continue

            header_cells = None
            year_columns = []

            # Search the first few rows for year headers with flexible matching
            for row in rows[:5]:
                potential_headers = row.find_all(['th', 'td'])
                temp_years = []
                seen_years = set()
                for i, cell in enumerate(potential_headers):
                    text = cell.get_text(strip=True)
                    match = re.search(r'20\d{2}|fiscal\s*20\d{2}|fy\s*20\d{2}|\d{4}', text, re.IGNORECASE)
                    if match:
                        year = re.search(r'20\d{2}|\d{4}', text).group(0) if re.search(r'20\d{2}|\d{4}', text) else match.group(0).replace('fiscal', '').replace('fy', '').strip()
                        if year not in seen_years:
                            seen_years.add(year)
                            temp_years.append((i, year))
                if temp_years:
                    header_cells = potential_headers
                    year_columns = temp_years
                    break

            if not year_columns:
                logger.warning(f"[MERCH] Could not detect year columns in table {processed_tables} — skipping.")
                continue

            year_indices = [idx for idx, _ in year_columns]
            year_map = {idx: year for idx, year in year_columns}

            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if not cells or len(cells) < 2:
                    continue

                category_name = cells[0].get_text(strip=True)
                if not self._is_merchandise_category(category_name):
                    continue

                revenue_by_year = {}
                if year_indices:
                    idx = year_indices[0]
                    year = year_map[idx]
                    try:
                        raw_text = cells[idx].get_text(strip=True)
                        if not raw_text or raw_text in ['$', '—', '-']:
                            if idx + 1 < len(cells):
                                raw_text = cells[idx + 1].get_text(strip=True)
                        amount = self._parse_amount(raw_text)
                        if amount is not None:
                            revenue_by_year[year] = amount
                    except IndexError:
                        pass

                if revenue_by_year:
                    # Check for total revenue labels dynamically
                    total_indicators = ['total', 'net', 'grand total', 'consolidated']
                    if any(indicator in category_name.lower() for indicator in total_indicators) and any(key in category_name.lower() for key in ['sales', 'revenue']):
                        revenue_data['total_revenue'] = revenue_by_year.get('2023', revenue_by_year.get('2022', revenue_by_year.get('2021')))
                    else:
                        if category_name not in revenue_data["revenue_breakdown"]:
                            revenue_data["revenue_breakdown"][category_name] = {}
                        revenue_data["revenue_breakdown"][category_name].update(revenue_by_year)
                        revenue_data["revenue_sources"].append({
                            "description": category_name,
                            "amounts": revenue_by_year,
                            "category": self._categorize_revenue_source(category_name)
                        })

        if revenue_data["revenue_breakdown"] or revenue_data["total_revenue"]:
            revenue_data["confidence_score"] = 0.9
            return revenue_data
        
        logger.warning("No valid merchandise category data extracted from any table.")
        return revenue_data
    
    def _extract_from_notes_tables(self):
        """
        Extract from tables in the notes section
        """
        revenue_data = {
            'total_revenue': None,
            'revenue_breakdown': {},
            'revenue_sources': [],
            'extraction_method': 'notes_tables',
            'confidence_score': 0.0
        }
        
        # Look for note sections first
        note_sections = self._find_note_sections()
        
        for section in note_sections:
            tables = section.find_all('table')
            
            for table in tables:
                table_text = table.get_text().lower()
                
                # Check if table contains revenue data
                if not any(keyword in table_text for keyword in ['revenue', 'sales', 'net sales']):
                    continue
                
                logger.info("Found revenue table in notes section")
                
                # Extract revenue data from this table
                extracted_data = self._extract_revenue_from_table(table)
                if extracted_data:
                    revenue_data.update(extracted_data)
                    revenue_data['confidence_score'] = 0.7
                    return revenue_data
        
        return revenue_data
    
    def _find_note_sections(self):
        """
        Find note sections in the filing
        """
        note_sections = []
        
        # Look for note headers
        note_patterns = [
            r'note\s+\d+.*revenue',
            r'note\s+\d+.*segment',
            r'disaggregated\s+revenue'
        ]
        
        for pattern in note_patterns:
            # Find elements that match note patterns
            matching_elements = self.soup.find_all(text=re.compile(pattern, re.IGNORECASE))
            
            for element in matching_elements:
                # Find the parent container (usually a div or section)
                parent = element.parent
                while parent and parent.name not in ['div', 'section', 'body']:
                    parent = parent.parent
                
                if parent:
                    note_sections.append(parent)
        
        return note_sections
    
    def _extract_revenue_from_table(self, table):
        """
        Generic method to extract revenue data from a table
        """
        revenue_data = {
            'revenue_breakdown': {},
            'revenue_sources': []
        }
        
        rows = table.find_all('tr')
        if len(rows) < 2:
            return None
        
        # Parse header row to identify year columns
        header_cells = rows[0].find_all(['th', 'td'])
        year_columns = []
        
        for i, cell in enumerate(header_cells):
            cell_text = cell.get_text().strip()
            year_match = re.search(r'20\d{2}', cell_text)
            if year_match:
                year_columns.append((i, year_match.group(0)))
        
        if not year_columns:
            return None
        
        # Extract data rows
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue
            
            # Get the row label (first cell)
            label_cell = cells[0]
            label = label_cell.get_text().strip()
            
            # Skip non-revenue rows
            if not self._is_revenue_row(label):
                continue
            
            # Extract amounts for each year
            for col_idx, year in year_columns:
                if col_idx >= len(cells):
                    continue
                
                amount_text = cells[col_idx].get_text().strip()
                amount = self._parse_amount(amount_text)
                
                if amount is not None:
                    key = f"{label}_{year}"
                    revenue_data['revenue_breakdown'][key] = amount
                    revenue_data['revenue_sources'].append({
                        'description': label,
                        'amount': amount,
                        'year': year,
                        'category': self._categorize_revenue_source(label)
                    })
        
        return revenue_data if revenue_data['revenue_breakdown'] else None
    
    def _extract_segment_revenue_from_table(self, table):
        """
        Extract segment revenue data (geographic or business segments)
        """
        revenue_data = {
            'revenue_breakdown': {},
            'revenue_sources': []
        }
        
        rows = table.find_all('tr')
        if len(rows) < 2:
            return None
        
        # Find header row with segments
        header_cells = rows[0].find_all(['th', 'td'])
        segment_columns = []
        
        for i, cell in enumerate(header_cells):
            cell_text = cell.get_text().strip()
            # Look for geographic or business segment names
            if any(segment in cell_text.lower() for segment in ['united states', 'canada', 'international', 'segment']):
                segment_columns.append((i, cell_text))
        
        if not segment_columns:
            return None
        
        # Extract revenue rows
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue
            
            row_label = cells[0].get_text().strip().lower()
            
            # Only process revenue rows
            if 'total revenue' not in row_label and 'net sales' not in row_label:
                continue
            
            # Extract amounts for each segment
            for col_idx, segment_name in segment_columns:
                if col_idx >= len(cells):
                    continue
                
                amount_text = cells[col_idx].get_text().strip()
                amount = self._parse_amount(amount_text)
                
                if amount is not None:
                    key = f"{segment_name}_revenue"
                    revenue_data['revenue_breakdown'][key] = amount
                    revenue_data['revenue_sources'].append({
                        'description': f"{segment_name} Revenue",
                        'amount': amount,
                        'segment': segment_name,
                        'category': 'geographic_segment'
                    })
        
        return revenue_data if revenue_data['revenue_breakdown'] else None
    
    def _extract_merchandise_revenue_from_table(self, table):
        rows = table.find_all("tr")
        if not rows:
            return None

        header_cells = None
        year_columns = []

        # Search the first few rows for year headers
        for row in rows[:5]:
            potential_headers = row.find_all(['th', 'td'])
            temp_years = []
            seen_years = set()
            for i, cell in enumerate(potential_headers):
                text = cell.get_text(strip=True)
                match = re.search(r'20\d{2}', text)
                if match:
                    year = match.group(0)
                    if year not in seen_years:
                        seen_years.add(year)
                        temp_years.append((i, year))
            if temp_years:
                header_cells = potential_headers
                year_columns = temp_years
                break

        if not year_columns:
            logger.warning("[MERCH] Could not detect year columns — skipping this table.")
            return None

        # Map column indices to year values
        year_indices = [idx for idx, _ in year_columns]
        year_map = {idx: year for idx, year in year_columns}

        revenue_data = {
            "revenue_breakdown": {},
            "revenue_sources": [],
        }

        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            category_name = cells[0].get_text(strip=True)
            if not self._is_merchandise_category(category_name):
                continue

            revenue_by_year = {}
            for idx in year_indices:
                year = year_map[idx]
                try:
                    # Get raw text and skip spacer cells
                    raw_text = cells[idx].get_text(strip=True)
                    if not raw_text or raw_text in ['$', '—', '-']:
                        if idx + 1 < len(cells):
                            raw_text = cells[idx + 1].get_text(strip=True)
                    amount = self._parse_numeric_value(raw_text)
                    if amount is not None:
                        revenue_by_year[year] = amount
                except IndexError:
                    continue

            if revenue_by_year:
                revenue_data["revenue_breakdown"][category_name] = revenue_by_year
                revenue_data["revenue_sources"].append({
                    "description": category_name,
                    "amounts": revenue_by_year,
                    "category": "merchandise_category"
                })

        if not revenue_data["revenue_breakdown"]:
            return None

        revenue_data["extraction_method"] = "table_parse"
        revenue_data["confidence_score"] = 1.0
        revenue_data["total_revenue"] = None
        return revenue_data

    
    def _is_revenue_row(self, label):
        """
        Check if a row label represents a revenue line item
        """
        label_lower = label.lower()
        revenue_indicators = [
            'total revenue', 'net sales', 'total net sales', 'revenue',
            'sales', 'total sales', 'net revenue', 'total revenues', 'net revenues'
        ]
        
        # Must contain a revenue indicator
        if not any(indicator in label_lower for indicator in revenue_indicators):
            return False
        
        # Exclude certain rows that aren't actual revenue
        exclude_terms = [
            'cost', 'expense', 'income', 'profit', 'margin', 'percentage',
            'ratio', 'growth', 'change', 'variance'
        ]
        
        if any(term in label_lower for term in exclude_terms):
            return False
        
        return True
    
    def _is_merchandise_category(self, category_name):
        """
        Check if a category name represents a merchandise category
        """
        category_lower = category_name.lower()
        revenue_categories = [
            'sales', 'revenue', 'net sales', 'total sales', 'product', 'service',
            'segment', 'geographic', 'category', 'foods', 'non-foods', 'fresh foods',
            'ancillary', 'warehouse', 'automotive', 'energy', 'services', 'leasing', 
            'iphone', 'mac', 'ipad', 'wearables', 'accessories', 'total net sales', 
            'net sales', 'product net sales'
        ]
        return any(cat in category_lower for cat in revenue_categories)
    
    def _parse_amount(self, amount_text):
        """
        Parse amount text into a numeric value
        """
        if not amount_text:
            return None
        
        # Clean the text
        amount_text = amount_text.replace(',', '').replace('$', '').replace('(', '-').replace(')', '').strip()
        
        # Handle units (e.g., thousands, millions)
        multiplier = 1
        if 'million' in amount_text.lower() or 'm' in amount_text.lower():
            multiplier = 1_000_000
            amount_text = amount_text.lower().replace('million', '').replace('m', '')
        elif 'thousand' in amount_text.lower() or 'k' in amount_text.lower():
            multiplier = 1_000
            amount_text = amount_text.lower().replace('thousand', '').replace('k', '')
        
        # Handle non-numeric values
        if amount_text in ['—', '-', '', 'N/A', 'n/a']:
            return None
        
        try:
            return float(amount_text) * multiplier
        except ValueError:
            logger.debug(f"Failed to parse amount: {amount_text}")
            return None
        
    def _categorize_revenue_source(self, description):
        """
        Categorize revenue source based on description
        """
        desc_lower = description.lower()
        
        if any(term in desc_lower for term in ['united states', 'canada', 'international', 'americas', 'europe', 'asia']):
            return 'geographic_segment'
        elif any(term in desc_lower for term in ['foods', 'non-foods', 'fresh foods', 'automotive', 'energy', 'services', 'product']):
            return 'merchandise_category'
        elif any(term in desc_lower for term in ['segment', 'division']):
            return 'business_segment'
        else:
            return 'other'


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

    def get_revenue_breakdown(self):
        """
        Extract revenue breakdown information from the filing using RevenueBreakdownExtractor
        """
        try:
            extractor = RevenueBreakdownExtractor(self.text, self.company)
            return extractor.extract_revenue_breakdown()
        except Exception as e:
            print(f"Error extracting revenue breakdown: {e}")
            
            return {
                'total_revenue': None,
                'revenue_breakdown': {},
                'revenue_sources': [],
                'extraction_method': 'failed',
                'confidence_score': 0.0,
                'error': str(e)
            }

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
                filename = self.get_html_file_name(filing_summary_xml, short_name)
                if filename is not None:
                    statement_names += [(short_name, filename)]
        else:
            print('No financial documents in this filing')

        if len(statement_names) == 0:
            print('No financial documents could be found. Likely need to \
            update constants in edgar.filing.Statements.')
            
        return statement_names



    @staticmethod
    def get_html_file_name(filing_summary_xml, report_short_name):
        '''
        Return the HtmlFileName (FILENAME) of the Report in FilingSummary.xml
        (filing_summary_xml) with ShortName in lowercase matching report_short_name
        e.g.
             report_short_name of consolidated statements of income matches
             CONSOLIDATED STATEMENTS OF INCOME
        '''
        reports = filing_summary_xml.find_all('report')
        for report in reports:
            short_name = report.find('shortname')
            if short_name is None:
                print('The following report has no ShortName element')
                print(report)
                continue
            # otherwise, get the text and keep procesing
            short_name = short_name.get_text().lower()
            # we want to make sure it matches, up until the end of the text
            if short_name == report_short_name.lower():
                filename = report.find('htmlfilename').get_text()
                return filename
        print(f'could not find anything for ShortName {report_short_name.lower()}')
        return None



    def get_income_statements(self):
        return self._get_financial_data(self.STATEMENTS.income_statements, False)

    def get_balance_sheets(self):
        return self._get_financial_data(self.STATEMENTS.balance_sheets, False)

    def get_cash_flows(self):
        return self._get_financial_data(self.STATEMENTS.cash_flows, False)