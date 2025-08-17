from edgar.stock import Stock
from uploader import upload_to_supabase
from datetime import datetime
import re
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date_flexible(date_string):
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
    def convert(obj):
        if isinstance(obj, list):
            return [convert(item) for item in obj]
        elif isinstance(obj, dict):
            converted = {}
            for k, v in obj.items():
                if k == "date" and isinstance(v, str):
                    parsed_date = parse_date_flexible(v)
                    converted[k] = parsed_date.strftime("%d-%m-%Y") if parsed_date else v
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
    if not data or not isinstance(data, list) or not isinstance(data[0], dict) or "date" not in data[0]:
        logger.warning(f"{data_type} data invalid or missing for {ticker}")
        return False
    return True

def extract_financial_data(filing, ticker, *years):
    def filter_reports_by_years(reports, valid_years):
        return [
            r for r in reports
            if parse_date_flexible(r.get("date", "")).year in valid_years
        ]

    try:
        logger.info(f"Checking reports for {ticker} in years: {years}")

        # ——— Income statement ———
        raw_income = filing.get_income_statements()
        if not raw_income or isinstance(raw_income, list):
            logger.error(f"No income-statement report found for {ticker}")
            return None, None, None, None
        income = convert_report(raw_income)
        income_reports = filter_reports_by_years(income["reports"], years)

        # ——— Balance sheet ———
        raw_balance = filing.get_balance_sheets()
        if not raw_balance or isinstance(raw_balance, list):
            logger.error(f"No balance-sheet report found for {ticker}")
            return None, None, None, None
        balance = convert_report(raw_balance)
        balance_reports = filter_reports_by_years(balance["reports"], years)

        # ——— Cash flow ———
        raw_cash = filing.get_cash_flows()
        if not raw_cash or isinstance(raw_cash, list):
            logger.error(f"No cash-flow report found for {ticker}")
            return None, None, None, None
        cash = convert_report(raw_cash)
        cash_reports = filter_reports_by_years(cash["reports"], years)

        logger.info(
            f"Filtered data counts for years {years} — "
            f"Income: {len(income_reports)}, Balance: {len(balance_reports)}, "
            f"Cash: {len(cash_reports)}"
        )

        if income_reports and balance_reports and cash_reports:
            return income_reports, balance_reports, cash_reports, max(years)

        logger.error(f"No valid reports found for {ticker} in {years}")
        return None, None, None, None

    except Exception as e:
        logger.error(f"Error extracting financial data for {ticker} {years}: {e}")
        return None, None, None, None

def process_company_filing(ticker, target_year):
    logger.info(f"Processing {ticker} 10-K targeting report year {target_year}")
    stock = Stock(ticker)

    for filing_year in (target_year, target_year + 1):
        company_name = None
        listed_exchange = None
        try:
            filing = stock.get_filing('annual', filing_year, 4)
            #filing.debug_print_shortnames() 
            try:
                listed_exchange = filing.extract_listed_exchanges()
                # company_website = filing.extract_company_website()
                filing.prepare_for_parsing()
            except Exception as e:
                logger.warning(f"Prune skipped (no summary?): {e}")
        except Exception as e:
            logger.warning(f"No 10-K found for {ticker} in {filing_year}: {e}")
            continue

        try:
            company_name = filing.extract_company_name()
            logger.info(f"Extracted company name: {company_name}, listed exhange {listed_exchange}")

            # function that locates R3/R5/R7 (or best-match) and filters rows to the report year
            income, balance, cash, report_year = extract_financial_data(filing, ticker, target_year)

            if income and balance and cash:
                logger.info(f"📦 Uploading {ticker} 10-K with report year {report_year} (filed in {filing_year})")
                ok = upload_to_supabase(ticker, report_year, 0, income, balance, cash, company_name, listed_exchange)
                return bool(ok)
            else:
                logger.info(f"Missing statements for {ticker} (filing year {filing_year}); trying next year…")
        except Exception as e:
            logger.exception(f"Failed while parsing {ticker} filing for {filing_year}: {e}")
            # try the next filing_year

    logger.error(f"No 10-K with report year {target_year} found for {ticker}")
    return False

def main():
    tickers = ['LVS']
    years = [2021]

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