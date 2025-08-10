# app/runner.py
import os
from typing import List, Dict, Any
from fastapi import logger
from .main import process_company_filing  # your existing function

def run_multiple(ticker: str, years: List[int]) -> List[Dict[str, Any]]:
    logger.info(f"Running parser for ticker={ticker}, years={years}")
    logger.info(f"Working dir files: {os.listdir('.')}")
    
    """
    Run the parser for multiple years for the given ticker.
    Returns a list of dicts with year and success status.
    """
    results = []
    for year in years:
        ok = process_company_filing(ticker, year)
        results.append({
            "year": year,
            "success": bool(ok)
        })
    return results
