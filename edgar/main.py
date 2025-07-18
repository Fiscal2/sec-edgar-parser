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
        '%d %B %Y'
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

def validate_revenue_breakdown(revenue_data, ticker):
    """
    Improved validation for revenue breakdown data
    """
    if not revenue_data or not isinstance(revenue_data, dict):
        logger.warning(f"Revenue breakdown data invalid or missing for {ticker}")
        return False
    
    # More lenient validation - accept if we have any revenue data
    has_breakdown = bool(revenue_data.get('revenue_breakdown'))
    has_sources = bool(revenue_data.get('revenue_sources'))
    extraction_method = revenue_data.get('extraction_method', 'unknown')
    confidence_score = revenue_data.get('confidence_score', 0)
    
    logger.info(f"Revenue validation for {ticker}: breakdown={has_breakdown}, sources={has_sources}, method={extraction_method}, confidence={confidence_score}")
    
    # Accept if we have either breakdown or sources, or if confidence > 0
    if has_breakdown or has_sources or confidence_score > 0:
        return True
    
    logger.warning(f"No meaningful revenue breakdown found for {ticker}")
    return False

def create_fallback_revenue_data(ticker):
    """
    Create fallback revenue data structure when extraction fails
    """
    return {
        'total_revenue': None,
        'revenue_breakdown': {},
        'revenue_sources': [],
        'extraction_method': 'fallback',
        'confidence_score': 0.0,
        'error': 'No revenue breakdown extracted'
    }

def extract_financial_data(filing, ticker, *years):
    def filter_reports_by_years(reports, valid_years):
        return [
            r for r in reports
            if parse_date_flexible(r.get("date", "")).year in valid_years
        ]

    try:
        logger.info(f"Checking reports for {ticker} in years: {years}")

        # Extract traditional financial statements
        income = convert_report(filing.get_income_statements())
        income_reports = filter_reports_by_years(income["reports"], years)

        balance = convert_report(filing.get_balance_sheets())
        balance_reports = filter_reports_by_years(balance["reports"], years)

        cash = convert_report(filing.get_cash_flows())
        cash_reports = filter_reports_by_years(cash["reports"], years)

        # Extract revenue breakdown with better error handling
        logger.info(f"Extracting revenue breakdown for {ticker}")
        try:
            revenue_breakdown = filing.get_revenue_breakdown()
            logger.info(f"Raw revenue breakdown result: {json.dumps(revenue_breakdown, indent=2, default=str)}")
        except Exception as e:
            logger.error(f"Error extracting revenue breakdown: {e}")
            revenue_breakdown = create_fallback_revenue_data(ticker)
        
        # Ensure revenue_breakdown is not None
        if revenue_breakdown is None:
            logger.warning(f"Revenue breakdown is None for {ticker}, using fallback")
            revenue_breakdown = create_fallback_revenue_data(ticker)
        
        logger.info(
            f"Filtered data counts for years {years} — "
            f"Income: {len(income_reports)}, Balance: {len(balance_reports)}, "
            f"Cash: {len(cash_reports)}, Revenue breakdown confidence: {revenue_breakdown.get('confidence_score', 0)}"
        )

        income_valid = validate_financial_data(income_reports, "income", ticker)
        balance_valid = validate_financial_data(balance_reports, "balance", ticker)
        cash_valid = validate_financial_data(cash_reports, "cash", ticker)
        
        # Always proceed with revenue data, even if validation fails
        # The upload function should handle empty/fallback data gracefully
        
        if income_valid and balance_valid and cash_valid:
            return income_reports, balance_reports, cash_reports, revenue_breakdown, max(years)

        logger.error(f"No valid reports found for {ticker} in {years}")
        return None, None, None, None, None

    except Exception as e:
        logger.error(f"Error extracting financial data for {ticker} {years}: {e}")
        return None, None, None, None, None

def process_company_filing(ticker, target_year):
    logger.info(f"Processing {ticker} 10-K targeting report year {target_year}")
    try:
        stock = Stock(ticker)

        # Try filing years: [target_year, target_year + 1]
        for filing_year in [target_year, target_year + 1]:
            filing = stock.get_filing('annual', filing_year, 4)

            if not filing:
                logger.warning(f"No 10-K filed in {filing_year} for {ticker}")
                continue

            # Try to extract reports *containing* the target report year
            income, balance, cash, revenue_breakdown, report_year = extract_financial_data(filing, ticker, target_year)

            if income and balance and cash:
                logger.info(f"📦 Uploading {ticker} 10-K with report year {report_year} (filed in {filing_year})")

                # Enhanced logging for revenue breakdown
                if revenue_breakdown:
                    logger.info(f"Revenue breakdown keys: {list(revenue_breakdown.keys())}")
                    logger.info(f"Extraction method: {revenue_breakdown.get('extraction_method', 'unknown')}")
                    logger.info(f"Confidence score: {revenue_breakdown.get('confidence_score', 0)}")
                    
                    if revenue_breakdown.get('revenue_breakdown'):
                        logger.info(f"Revenue breakdown categories: {len(revenue_breakdown['revenue_breakdown'])}")
                        # Log the actual breakdown data
                        for key, value in list(revenue_breakdown['revenue_breakdown'].items())[:5]:
                            logger.info(f"  {key}: {value}")
                    
                    if revenue_breakdown.get('revenue_sources'):
                        logger.info(f"Revenue sources: {len(revenue_breakdown['revenue_sources'])}")
                        # Log some sample revenue sources with correct key
                        for source in list(revenue_breakdown.get('revenue_sources', []))[:3]:
                            logger.info(f"  - {source.get('description', 'Unknown')}: {source.get('amount', 'N/A')}")
                    
                    # Log any errors
                    if revenue_breakdown.get('error'):
                        logger.warning(f"Revenue extraction error: {revenue_breakdown['error']}")
                else:
                    logger.warning(f"No revenue breakdown data for {ticker}")

                # Always attempt upload, even with empty/fallback revenue data
                return upload_to_supabase(ticker, report_year, 0, income, balance, cash, revenue_breakdown)

        logger.error(f"No 10-K with report year {target_year} found for {ticker}")
        return False

    except Exception as e:
        logger.error(f"Failed to process {ticker} targeting {target_year}: {e}", exc_info=True)
        return False

def main():
    tickers = ['AAPL']
    years = [2024]

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