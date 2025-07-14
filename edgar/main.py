from edgar.stock import Stock
from uploader import upload_to_supabase
from datetime import datetime
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date_flexible(date_string):
    """Parse dates with multiple format attempts"""
    if not date_string:
        return None
    date_string = date_string.strip()

    date_patterns = [
        '%d-%m-%Y',
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%B %d, %Y',
        '%b %d, %Y',
        '%b. %d, %Y',
        '%d %b %Y',
        '%d %B %Y',
    ]

    for pattern in date_patterns:
        try:
            return datetime.strptime(date_string, pattern)
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_string}")
    return None

def convert_report(report):
    """Convert financial data to clean, serializable form"""
    def convert(obj):
        if isinstance(obj, list):
            return [convert(item) for item in obj]
        elif isinstance(obj, dict):
            converted = {}
            for k, v in obj.items():
                if k == "date" and isinstance(v, str):
                    parsed_date = parse_date_flexible(v)
                    if parsed_date:
                        converted[k] = parsed_date.strftime("%d-%m-%Y")
                    else:
                        converted[k] = v
                        logger.warning(f"Could not parse date '{v}', keeping original format")
                else:
                    converted[k] = convert(v)
            return converted
        elif isinstance(obj, datetime):
            return obj.strftime("%d-%m-%Y")
        elif hasattr(obj, "__dict__"):
            return convert(obj.__dict__)
        else:
            return obj
    return convert(report)

def validate_financial_data(data, data_type, ticker):
    if not data:
        logger.warning(f"No {data_type} data found for {ticker}")
        return False
    if not isinstance(data, list):
        logger.warning(f"{data_type} data is not a list for {ticker}")
        return False
    if len(data) == 0:
        logger.warning(f"{data_type} data is empty list for {ticker}")
        return False
    if not isinstance(data[0], dict):
        logger.warning(f"First {data_type} item is not a dict for {ticker}")
        return False
    if "date" not in data[0]:
        logger.warning(f"No date field in {data_type} data for {ticker}")
        return False
    return True

def extract_financial_data(filing, ticker, year):
    try:
        logger.info(f"Extracting income statements for {ticker} {year}")
        income = convert_report(filing.get_income_statements())
        income_reports = income["reports"]

        logger.info(f"Extracting balance sheets for {ticker} {year}")
        balance = convert_report(filing.get_balance_sheets())
        balance_reports = balance["reports"]

        logger.info(f"Extracting cash flows for {ticker} {year}")
        cash = convert_report(filing.get_cash_flows())
        cash_reports = cash["reports"]

        logger.info(f"Extracted data counts - Income: {len(income_reports)}, Balance: {len(balance_reports)}, Cash: {len(cash_reports)}")

        income_valid = validate_financial_data(income_reports, "income", ticker)
        balance_valid = validate_financial_data(balance_reports, "balance", ticker)
        cash_valid = validate_financial_data(cash_reports, "cash", ticker)

        if not (income_valid and balance_valid and cash_valid):
            logger.error(f"Data validation failed for {ticker} {year}")
            return None, None, None

        return income_reports, balance_reports, cash_reports

    except Exception as e:
        logger.error(f"Error extracting financial data for {ticker} {year}: {e}")
        return None, None, None

def process_company_filing(ticker, year):
    logger.info(f"Processing {ticker} 10-K for {year}")
    try:
        stock = Stock(ticker)
        filing = stock.get_filing('annual', year, 4)

        if not filing:
            logger.warning(f"No 10-K found for {ticker} {year}")
            return False

        income, balance, cash = extract_financial_data(filing, ticker, year)

        if not (income and balance and cash):
            logger.error(f"Failed to extract complete financial data for {ticker} {year}")
            return False

        report_date_str = income[0].get("date")
        if not report_date_str:
            logger.error(f"Missing date in income statement for {ticker} {year}")
            return False

        parsed_date = parse_date_flexible(report_date_str)
        if not parsed_date:
            logger.error(f"Could not parse date '{report_date_str}' for {ticker} {year}")
            return False

        report_year = parsed_date.year
        logger.info(f"📦 Uploading {ticker} 10-K for report year {report_year} (filed in {year})")

        upload_success = upload_to_supabase(ticker, report_year, 0, income, balance, cash)
        return upload_success

    except Exception as e:
        logger.error(f"Failed to process {ticker} 10-K {year}: {e}")
        return False

def main():
    tickers = ['AAPL', 'GOOG', 'AMZN', 'JPM', 'QCOM']
    years = [2022]

    results = {
        'successful': [],
        'failed': [],
        'total_processed': 0
    }

    for ticker in tickers:
        logger.info(f"\nStarting annual filings for {ticker}...")
        for year in years:
            results['total_processed'] += 1
            success = process_company_filing(ticker, year)

            if success:
                results['successful'].append(f"{ticker} {year}")
                logger.info(f"✅ Successfully processed {ticker} {year}")
            else:
                results['failed'].append(f"{ticker} {year}")
                logger.error(f"❌ Failed to process {ticker} {year}")

    print("\n" + "=" * 50)
    print("PROCESSING SUMMARY")
    print("=" * 50)
    print(f"Total processed: {results['total_processed']}")
    print(f"Successful: {len(results['successful'])}")
    print(f"Failed: {len(results['failed'])}")

    if results['successful']:
        print("\nSuccessful uploads:")
        for item in results['successful']:
            print(f"  ✅ {item}")

    if results['failed']:
        print("\nFailed uploads:")
        for item in results['failed']:
            print(f"  ❌ {item}")

    success_rate = (len(results['successful']) / results['total_processed']) * 100
    print(f"\nSuccess rate: {success_rate:.1f}%")

if __name__ == "__main__":
    main()