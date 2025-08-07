'''
Handles financial logic
'''
import re
from bs4 import BeautifulSoup
from json import JSONEncoder
from datetime import datetime
from dateutil.parser import parse as parse_date

class FinancialReportEncoder(JSONEncoder):
        
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return o.__dict__



class FinancialElement:
    '''
    Models financial elements
    '''
    def __init__(self, label, value):
        self.label = label
        self.value = value

    def __repr__(self):
        return str(self.__dict__)



class FinancialInfo:
    '''
    Models financial data provided in a financial report
    financial elements are stored in a map to retain flexibility
    '''
    def __init__(self, date, months, map):
        '''
        :param date: date of the information
        :param months: number of months that it covers (None if balance sheet)
        :param map: map of XBRL element name to value
        '''
        self.date = date
        self.months = months
        self.map = map

    def __repr__(self):
        return str(self.__dict__)



class FinancialReport:
    '''
    Models financial reports from an edgar filing
    financial elements are stored in a map to retain flexibility
    '''
    def __init__(self, company, date_filed, reports=[]):
        '''
        :param company: identifier for a company (not using the term "symbol"
            because not all companies that file on edgar are publicly traded)
        :param reports: list of FinancialInfo objects
        '''
        self.company = company
        self.date_filed = date_filed
        self.reports = reports

    def add_financial_info(self, financial_info: FinancialInfo):
        self.reports.append(financial_info)

    def __repr__(self):
        return str(self.__dict__)





class MetaDataParsingException(Exception):
    pass

# https://pypi.org/project/python-xbrl/

'''
XBRL rules for us-gaap namespace are found at the site below
https://xbrl.us/data-rule/dqc_0015-le/

spreadsheet is in docs folder

Notes:
 - only accept us-gaap based filings

For 10-K or 10-Q
1. get 10-K/10-Q filing from filings list
2. for each filing, in the filing text doc, find the FilingSummary.xml
3. In FilingSummary.MyReports, find the Reports with ShortNames matching
   what's set in STATEMENT_SHORT_NAMES (lower case)
4. get the HtmlFileName of the Report
5. find the DOCUMENT with the given FILENAME in HtmlFileName
The next part differs based on 10-K and 10-Q
6. in the TEXT.html.body, get the data in the first table (class="report") and 
   parse. 
   a. Exclude the first row (title and 12 Months Ended text)
   b. Should have four columns, with the last three representing the 
      current year, last year, and two years ago (order may vary).
   c. Years will be in th elements (class="th"), data in the td elements with
      class="nump"
   d. The first td in each row will tell us the us-gaap namespace elementName.
      This will be in the onclick of the a tag in the td, e.g.
        onclick="top.Show.showAR( this, 'defref_us-gaap_CostOfGoodsSold'...
      Some might not have us-gaap, e.g.
        defref_air_OperatingIncomeLossIncludingIncomeLossFromEquityMethodInvestments
      though this should be defref_us-gaap_OperatingIncomeLoss
   e. millions? Assume yes. th in first row with class="t1", div.strong:
        Consolidated Statements Of Operations - USD ($)<br> shares in Millions,
        $ in Millions
'''



def get_financial_report(company, date_filed, financial_html_text):
    '''
    Returns a FinancialReport from html-structured financial data
    
    :param company: identifier of the company that the financial_html_text
        belongs to (can be the company's stock symbol, for example)
    :param date_filed: datetime representing ACCEPTANCE-DATETIME of Filing
    :param financial_html_text: html-structured financial data from an annual
        or quarterly Edgar filing
    '''
    financial_info = _process_financial_info(financial_html_text)
    financial_report = FinancialReport(company, date_filed, financial_info)
    return financial_report



def _process_financial_info(financial_html_text):
    '''
    Return a list of FinancialInfo objects from html-structured financial data

    :param financial_html_text: html-structured financial data from an annual
        or quarterly Edgar filing
    '''
    source_soup = BeautifulSoup(financial_html_text, 'html.parser')
    report = source_soup.find('table', {'class': 'report'})
    if report is None:
        return []

    rows = report.find_all('tr')
    financial_info = []

    dates, period_units, unit_text = _get_statement_meta_data(rows)

    for i, date in enumerate(dates):
        dt = datetime.strptime(date, '%b. %d, %Y')
        financial_info.append(FinancialInfo(dt, period_units[i], {}))

    # find the first row that actually contains numeric data,
    # instead of assuming it's always rows[2:]
    start_idx = next(
        (i for i, row in enumerate(rows)
         if row.find('td', class_='nump') or row.find('td', class_='num')),
        2
    )

    for row in rows[start_idx:]:
        data = row.find_all('td')
        if not data:
            continue

        xbrl_element = None
        label = None
        numeric_data_available = False
        value_index = 0

        for td in data:
            info_text = td.get_text().strip()
            class_list = td.get('class', [])

            processed_financial_value = None

            # if it's the first cell (no numbers seen yet) and either un-classed or class="text",
            # treat it as the label
            if not numeric_data_available and info_text and (not class_list or 'text' in class_list):
                xbrl_element = _process_xbrl_element(td)
                label = info_text
                continue

            if 'pl' in class_list:
                # pl class indicates the td is the financial label
                xbrl_element = _process_xbrl_element(td)
                # print(xbrl_element)
                label = info_text

            elif 'nump' in class_list or 'num' in class_list:
                # nump class indicates td, and so more generally, the row, has numeric data
                numeric_data_available = True
                if xbrl_element is not None:
                    processed_financial_value = _process_financial_value(
                        info_text, xbrl_element, unit_text
                    )

            elif 'text' in class_list:
                if numeric_data_available and xbrl_element is not None:
                    # this corner case occurs when a given element appears sparsely
                    processed_financial_value = _process_financial_value(
                        info_text, xbrl_element, unit_text
                    )
                # else:
                #     # super label (abstract - no financial data)
                #     print(xbrl_element)

            if processed_financial_value is not None:
                # print(index)
                if value_index >= len(financial_info):
                    print(f"[WARN] Skipping value at index {value_index} — "
                          f"exceeds financial_info length {len(financial_info)}")
                else:
                    financial_info_map = financial_info[value_index].map
                    if xbrl_element and xbrl_element not in financial_info_map:
                        # handles adjustment details
                        # e.g. https://www.sec.gov/Archives/edgar/data/867773/0000867773-18-000082.txt
                        financial_info_map[xbrl_element] = FinancialElement(
                            label, processed_financial_value
                        )
                value_index += 1  # advance only when we store a value

    # Remove empty reports
    financial_info = [fi for fi in financial_info if fi.map]
    return financial_info



def _get_statement_meta_data(rows):
    '''
    Returns the dates, period_units, unit_text given the html table rows of a
    financial statement filing

    :return: tuple of:
        dates - list of the different dates of the filing,
        period_units - list of the period (in months) that each date covers,
        unit_text - text that tells us the unit of shares and dollars being
            used in the filing
    '''
    dates = []
    period_units = []
    unit_text = None
    is_snapshot = False

    title_repeat = 0
    found_dates = []

    # Combine the first two rows of headers
    header_rows = rows[:2]
    header_text = []

    for row in header_rows:
        cells = row.find_all(['th', 'td'])
        header_text.append([cell.get_text(strip=True) for cell in cells])

    flat_text = [cell for row in header_text for cell in row]

    # Look for unit_text like "shares in Thousands, $ in Millions"
    for text in flat_text:
        if "shares in" in text.lower() or "$ in" in text.lower():
            unit_text = text
            break

    # Look for column headers with recognizable dates or period phrases
    for text in flat_text:
        text_lower = text.lower()
        try:
            if "ended" in text_lower or "as of" in text_lower:
                # Try to extract month count from text
                months = None
                m = re.search(r"(\d+)\s+month", text_lower)
                if m:
                    months = int(m.group(1))
                period_units.append(months if months else 12)  # Default to 12
            elif re.search(r'\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b', text_lower):
                # Likely a date
                dt = parse_date(text, fuzzy=True).strftime("%b. %d, %Y")
                found_dates.append(dt)
        except Exception:
            continue

    # If we found dates, pair them with the periods
    if found_dates:
        dates = found_dates
        if len(period_units) < len(dates):
            period_units = [12] * len(dates)
    else:
        raise MetaDataParsingException("No recognizable dates found in header rows.")

    if len(dates) != len(period_units):
        raise MetaDataParsingException(f"Mismatch in dates and period_units: {len(dates)} vs {len(period_units)}")

    return dates, period_units, unit_text



def _process_period(info_text):
    '''
    Returns the number of months given a financial reporting period
    
    :param info_text: a reporting period, e.g. "12 Months Ended"
    '''
    return int(re.sub('[^0-9]', '', info_text))



def _process_xbrl_element(info):
    '''
    Returns the name of the XBRL element in info (html BeautifulSoup).
    Tries, in order:
      1. the onclick of a child <a> (defref_…)
      2. an id or name attribute on the <td> itself
      3. a slugified version of the visible text
    '''
    # 1) Try the anchor’s onclick
    anchor = info.find('a')
    if anchor:
        onclick = anchor.attrs.get('onclick', '')
        m = re.search(r"defref_([^']+)'", onclick)
        if m:
            return m.group(1)

    # 2) Try id/name on the <td> itself
    if info.has_attr('id'):
        return info['id']
    if info.has_attr('name'):
        return info['name']

    # 3) Last resort: slugify the label text
    txt = info.get_text(separator=' ').strip().lower()
    slug = re.sub(r'[^a-z0-9]+', '_', txt).strip('_')
    return slug or None



def _process_financial_value(text, xbrl_element, unit_text):
    '''
    Returns float representation of text after stripping special characters

    :param text: the monetary value, which if in brackets, is negative
    :param xbrl_element: text of html element that contains xbrl info
        for the value of the text (i.e. the context)
    :param unit_text: text of the form "x in y" where
        x is either "shares" or "$"
        y is either "thousands", "millions", or "billions"
    '''
    is_negative = True if '(' in text else False
    # strip special characters
    amount_text = re.sub('[^0-9\\.]', '', text)
    value = None

    try:
        amount = float(amount_text)
        value = -amount if is_negative else amount
        u = (unit_text or "").lower()

        # handle units
        if('PerShare' in xbrl_element):
            value = value # no change
        elif (('Shares' in xbrl_element and 'shares in billions' in u)
            or ('Shares' not in xbrl_element and '$ in billions' in u)):
            value = value * 1000000000
        elif (('Shares' in xbrl_element and 'shares in millions' in u)
            or ('Shares' not in xbrl_element and '$ in millions' in u)):
            value = value * 1000000
        elif (('Shares' in xbrl_element and 'shares in thousands' in u)
            or ('Shares' not in xbrl_element and '$ in thousands' in u)):
            value = value * 1000

    except ValueError:
        print('Warning: {} (from {}) is not numeric even after removing special characters () - ignoring'.format(text, xbrl_element, amount_text))

    return value