import requests
from supabase_client import supabase
import json
import time
import logging

logger = logging.getLogger(__name__)

from bs4 import BeautifulSoup
import requests

def upload_to_supabase(ticker, year, quarter, income, balance, cash, company_name=None, listed_exchange=None):
    """
    Upload financial data to Supabase with comprehensive error handling
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        # Validate inputs
        if not all([ticker, year is not None, quarter is not None, income, balance, cash]):
            logger.error(f"Missing required data for {ticker} {year} Q{quarter}")
            return False
        
        # Check for duplicates
        logger.info(f"Checking for existing data: {ticker} {year} Q{quarter}")
        existing = supabase.table("financials") \
            .select("id, company_name, listed_exchange") \
            .eq("ticker", ticker) \
            .eq("year", year) \
            .eq("quarter", quarter) \
            .execute()
        
        if existing.data and len(existing.data) > 0:
            row = existing.data[0]
            needs_update = False
            payload = {}

            # Only update if missing
            if not row.get("company_name") and company_name:
                payload["company_name"] = company_name.upper()
                needs_update = True

            if not row.get("listed_exchange") and listed_exchange:
                payload["listed_exchange"] = listed_exchange
                needs_update = True

            # If nothing to update, skip entirely
            if not needs_update:
                logger.info(f"Skipping {ticker} Q{quarter} {year} — already has company_name and listed_exchange.")
                return True

            logger.info("Existing record missing one or more fields — will update via upsert.")
            result = supabase.table("financials").update(payload) \
                .eq("ticker", ticker) \
                .eq("year", year) \
                .eq("quarter", quarter) \
                .execute()

            if result.data:
                logger.info(f"✅ Successfully updated {ticker} Q{quarter} {year}")
                return True
            else:
                logger.error(f"❌ Update failed for {ticker} Q{quarter} {year}")
                return False
            
            # Insert path — validate inputs
        if not all([ticker, year is not None, quarter is not None, income, balance, cash]):
            logger.error(f"Missing required data for {ticker} {year} Q{quarter}")
            return False

        
        # Validate data structure before JSON conversion
        try:
            # Test JSON serialization
            json.dumps(income)
            json.dumps(balance)
            json.dumps(cash)
        except (TypeError, ValueError) as e:
            logger.error(f"Data serialization error for {ticker} {year} Q{quarter}: {e}")
            return False
        
        # Prepare payload
        payload = {
            "ticker": ticker,
            "year": year,
            "quarter": quarter,
            "income_statement": json.dumps(income),
            "balance_sheet": json.dumps(balance),
            "cash_flow": json.dumps(cash),
        }

        if company_name:
            payload["company_name"] = company_name.upper()

        if listed_exchange:
            payload["listed_exchange"] = listed_exchange

        # if company_website:
        #     payload["company_website"] = company_website

        # Extract total revenue from income statement
        total_revenue = None
        if income and len(income) > 0:
            for item in income:
                if item.get('date') and str(year) in item.get('date'):
                    map_data = item.get('map', {})
                    revenue_labels = ['total revenue', 'net sales', 'total net sales', 'revenue', 'total revenues', 'consolidated revenue']
                    for label in revenue_labels:
                        if label in map_data:
                            total_revenue = map_data[label].get('value')
                            if total_revenue is not None:
                                logger.info(f"Extracted total revenue from income statement: {total_revenue} for {year}")
                                break
                    if total_revenue is not None:
                        break


        # Log data sizes for debugging
        logger.info(f"Payload sizes - Income: {len(payload['income_statement'])}, "
                   f"Balance: {len(payload['balance_sheet'])}, "
                   f"Cash: {len(payload['cash_flow'])}, ")
        
        # Upload to Supabase
        logger.info(f"Uploading {ticker} Q{quarter} {year} to Supabase...")
        result = supabase.table("financials").upsert(payload, on_conflict="ticker,year,quarter").execute()
        
        # Check result
        if result.data:
            logger.info(f"✅ Successfully uploaded {ticker} Q{quarter} {year}")
            
            # Log some sample data for verification
            if income and len(income) > 0:
                sample_income = income[0]
                logger.info(f"Sample income data: date={sample_income.get('date')}, "
                           f"fields={len(sample_income.get('map', {}))}")

            
            time.sleep(1 / 9)  # Rate limiting
            return True
        else:
            error_msg = getattr(result, 'error_message', 'Unknown error')
            logger.error(f"❌ Upload failed for {ticker} Q{quarter} {year}: {error_msg}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error uploading {ticker} Q{quarter} {year}: {e}")
        return False

def validate_financial_data_structure(data, data_type):
    """
    Validate the structure of financial data before upload
    
    Args:
        data: Financial data to validate
        data_type: Type of data ('income', 'balance', 'cash')
    
    Returns:
        bool: True if valid, False otherwise
    """
    if not isinstance(data, list):
        logger.error(f"{data_type} data is not a list")
        return False
    
    if len(data) == 0:
        logger.error(f"{data_type} data is empty")
        return False
    
    # Check structure of first item
    first_item = data[0]
    if not isinstance(first_item, dict):
        logger.error(f"First {data_type} item is not a dictionary")
        return False
    
    # Check for required fields
    required_fields = ['date', 'map']
    for field in required_fields:
        if field not in first_item:
            logger.error(f"Missing required field '{field}' in {data_type} data")
            return False
    
    # Check map structure
    if not isinstance(first_item['map'], dict):
        logger.error(f"'map' field in {data_type} data is not a dictionary")
        return False
    
    if len(first_item['map']) == 0:
        logger.warning(f"'map' field in {data_type} data is empty")
        return False
    
    # Validate individual map entries
    for key, value in first_item['map'].items():
        if not isinstance(value, dict):
            logger.error(f"Map entry '{key}' in {data_type} data is not a dictionary")
            return False
        
        if 'label' not in value or 'value' not in value:
            logger.error(f"Map entry '{key}' in {data_type} data missing label or value")
            return False
    
    return True

def upload_to_supabase_with_validation(ticker, year, quarter, income, balance, cash, company_name, listed_exchange=None):
    """
    Enhanced upload function with data validation
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    # Validate all data structures
    if not validate_financial_data_structure(income, 'income'):
        return False
    if not validate_financial_data_structure(balance, 'balance'):
        return False
    if not validate_financial_data_structure(cash, 'cash'):
        return False
    # Proceed with upload
    return upload_to_supabase(ticker, year, quarter, income, balance, cash, company_name, listed_exchange)

def get_existing_data(ticker, year, quarter):
    """
    Get existing financial data from Supabase
    
    Returns:
        dict or None: Existing data if found, None otherwise
    """
    try:
        result = supabase.table("financials") \
            .select("*") \
            .eq("ticker", ticker) \
            .eq("year", year) \
            .eq("quarter", quarter) \
            .execute()
        
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
        
    except Exception as e:
        logger.error(f"Error fetching existing data for {ticker} {year} Q{quarter}: {e}")
        return None

def update_existing_data(ticker, year, quarter, income, balance, cash, listed_exchange=None):
    """
    Update existing financial data in Supabase
    
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        payload = {
            "income_statement": json.dumps(income),
            "balance_sheet": json.dumps(balance),
            "cash_flow": json.dumps(cash),
        }

        if listed_exchange:
            payload["listed_exchange"] = json.dumps(listed_exchange)
        
        result = supabase.table("financials") \
            .update(payload) \
            .eq("ticker", ticker) \
            .eq("year", year) \
            .eq("quarter", quarter) \
            .execute()
        
        if result.data:
            logger.info(f"✅ Updated existing data for {ticker} Q{quarter} {year}")
            return True
        else:
            logger.error(f"❌ Update failed for {ticker} Q{quarter} {year}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error updating {ticker} Q{quarter} {year}: {e}")
        return False