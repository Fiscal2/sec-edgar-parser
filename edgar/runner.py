# app/runner.py
import os
from typing import List, Dict, Any
from .main import process_company_filing  # your existing function
import logging
logger = logging.getLogger(__name__)

def run_multiple(ticker: str, years: List[int]) -> List[Dict[str, Any]]:
    logger.info(f"Running parser for ticker={ticker}, years={years}")
    logger.info(f"Working dir files: {os.listdir('.')}")
    
    """
    Run the parser for multiple years for the given ticker.
    Returns a list of dicts with year and success status.
    """

    results = []
    for year in years:
        try:
            ok = process_company_filing(ticker, year)
            results.append({"year": year, "success": bool(ok)})
        except Exception as e:
            logger.exception(f"Error processing {ticker} for {year}: {e}")
            results.append({"year": year, "success": False, "error": str(e)})
    return results

