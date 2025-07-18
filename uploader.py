from supabase_client import supabase
import json
import time
import logging

logger = logging.getLogger(__name__)

def upload_to_supabase(ticker, year, quarter, income, balance, cash, revenue_breakdown):
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
            .select("id") \
            .eq("ticker", ticker) \
            .eq("year", year) \
            .eq("quarter", quarter) \
            .execute()
        
        if existing.data and len(existing.data) > 0:
            logger.info(f"⏩ Skipping {ticker} Q{quarter} {year} — already in Supabase.")
            return True  # Consider this a success since data already exists
        
        # Validate data structure before JSON conversion
        try:
            # Test JSON serialization
            json.dumps(income)
            json.dumps(balance)
            json.dumps(cash)
            if revenue_breakdown:
                json.dumps(revenue_breakdown)
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
            "revenue_breakdown": json.dumps(revenue_breakdown)
        }

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


        rb = revenue_breakdown.get("revenue_breakdown") if revenue_breakdown else None
        if isinstance(rb, dict) and len(rb) > 0:
            category_count = len(rb)
            logger.info(f"Including revenue breakdown with {category_count} categories")
            payload["revenue_breakdown"] = json.dumps(revenue_breakdown)
        else:
            logger.warning("Revenue breakdown is missing or empty")
            payload["revenue_breakdown"] = None

        # Log data sizes for debugging
        logger.info(f"Payload sizes - Income: {len(payload['income_statement'])}, "
                   f"Balance: {len(payload['balance_sheet'])}, "
                   f"Cash: {len(payload['cash_flow'])}, "
                   f"Revenue breakdown: {len(payload.get('revenue_breakdown', '') or '')}")
        
        # Upload to Supabase
        logger.info(f"Uploading {ticker} Q{quarter} {year} to Supabase...")
        result = supabase.table("financials").insert(payload).execute()
        
        # Check result
        if result.data:
            logger.info(f"✅ Successfully uploaded {ticker} Q{quarter} {year}")
            
            # Log some sample data for verification
            if income and len(income) > 0:
                sample_income = income[0]
                logger.info(f"Sample income data: date={sample_income.get('date')}, "
                           f"fields={len(sample_income.get('map', {}))}")

                
            # Log revenue breakdown summary
            rb = revenue_breakdown.get("revenue_breakdown") if revenue_breakdown else None
            if isinstance(rb, dict) and len(rb) > 0:
                logger.info(f"Revenue breakdown summary:")
                logger.info(f"  - Method: {revenue_breakdown.get('extraction_method', 'unknown')}")
                logger.info(f"  - Confidence: {revenue_breakdown.get('confidence_score', 0)}")
                logger.info(f"  - Categories: {len(revenue_breakdown.get('revenue_breakdown', {}))}")
                
                # Show top 3 revenue sources
                for i, source in enumerate(revenue_breakdown.get('revenue_sources', [])[:3]):
                    logger.info(f"  - Source {i+1}: {source.get('description', 'Unknown')}")
            
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

def validate_revenue_breakdown_structure(revenue_data):
    """
    Validate the structure of revenue breakdown data
    
    Args:
        revenue_data: Revenue breakdown data to validate
    
    Returns:
        bool: True if valid, False otherwise
    """
    if not isinstance(revenue_data, dict):
        logger.error("Revenue breakdown data is not a dictionary")
        return False
    
    required_fields = ['revenue_breakdown', 'revenue_sources', 'extraction_method', 'confidence_score']
    for field in required_fields:
        if field not in revenue_data:
            logger.warning(f"Missing field '{field}' in revenue breakdown data")
    
    # Check revenue_breakdown structure
    if 'revenue_breakdown' in revenue_data and not isinstance(revenue_data['revenue_breakdown'], dict):
        logger.error("'revenue_breakdown' field is not a dictionary")
        return False
    
    # Check revenue_sources structure
    if 'revenue_sources' in revenue_data:
        if not isinstance(revenue_data['revenue_sources'], list):
            logger.error("'revenue_sources' field is not a list")
            return False
        
        for i, source in enumerate(revenue_data['revenue_sources']):
            if not isinstance(source, dict):
                logger.error(f"Revenue source {i} is not a dictionary")
                return False
            
            if 'description' not in source:
                logger.warning(f"Revenue source {i} missing description")
    
    # Check confidence score
    if 'confidence_score' in revenue_data:
        try:
            score = float(revenue_data['confidence_score'])
            if not (0.0 <= score <= 1.0):
                logger.warning(f"Confidence score {score} is not between 0.0 and 1.0")
        except (ValueError, TypeError):
            logger.warning("Confidence score is not a valid number")
    
    return True

def upload_to_supabase_with_validation(ticker, year, quarter, income, balance, cash, revenue_breakdown):
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
    
    # # Validate revenue breakdown if provided
    # if revenue_breakdown and not validate_revenue_breakdown_structure(revenue_breakdown):
    #     logger.warning("Revenue breakdown validation failed, proceeding without it")
    #     revenue_breakdown = None

    # Proceed with upload
    return upload_to_supabase(ticker, year, quarter, income, balance, cash, revenue_breakdown)

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

def update_existing_data(ticker, year, quarter, income, balance, cash, revenue_breakdown=None):
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

        # Add revenue breakdown if available
        if revenue_breakdown:
            payload["revenue_breakdown"] = json.dumps(revenue_breakdown)
        
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

def get_revenue_breakdown_summary(ticker, year, quarter):
    """
    Get a summary of revenue breakdown for a specific company/year/quarter
    
    Returns:
        dict: Summary of revenue breakdown data
    """
    try:
        result = supabase.table("financials") \
            .select("revenue_breakdown") \
            .eq("ticker", ticker) \
            .eq("year", year) \
            .eq("quarter", quarter) \
            .execute()
        
        if result.data and len(result.data) > 0:
            revenue_data = result.data[0].get('revenue_breakdown')
            if revenue_data:
                try:
                    parsed_data = json.loads(revenue_data)
                    return {
                        'ticker': ticker,
                        'year': year,
                        'quarter': quarter,
                        'extraction_method': parsed_data.get('extraction_method', 'unknown'),
                        'confidence_score': parsed_data.get('confidence_score', 0),
                        'revenue_categories': len(parsed_data.get('revenue_breakdown', {})),
                        'revenue_sources': len(parsed_data.get('revenue_sources', [])),
                        'top_revenue_sources': [
                            source.get('description', 'Unknown') 
                            for source in parsed_data.get('revenue_sources', [])[:5]
                        ]
                    }
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse revenue breakdown JSON for {ticker} {year} Q{quarter}")
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting revenue breakdown summary for {ticker} {year} Q{quarter}: {e}")
        return None