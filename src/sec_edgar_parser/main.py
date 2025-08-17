#!/usr/bin/env python3
"""
Modern SEC EDGAR Parser Main Module

This module demonstrates the new service-oriented architecture while maintaining
backward compatibility with existing code.
"""

import logging
from typing import List

from .services import MainService, RunnerService

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the SEC EDGAR Parser"""
    # Example usage with the new architecture
    tickers = ['AMZN']
    years = [2021, 2022, 2023, 2024]
    
    # Use the modern service
    service = MainService()
    
    try:
        results = service.process_multiple_companies(tickers, years)
        
        # Print results
        print("\n" + "=" * 50)
        print("MODERN ARCHITECTURE RESULTS")
        print("=" * 50)
        print(f"Total processed: {results['total_processed']}")
        print(f"Successful: {len(results['successful'])}")
        print(f"Failed: {len(results['failed'])}")
        print(f"Success rate: {results['success_rate']:.1f}%")
        
        if results['successful']:
            print("\nSuccessful processing:")
            for item in results['successful']:
                print(f"  ✅ {item}")
        
        if results['failed']:
            print("\nFailed processing:")
            for item in results['failed']:
                print(f"  ❌ {item}")
        
        if 'duration' in results:
            print(f"\nTotal duration: {results['duration']}")
            
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        print(f"❌ Error: {e}")
    
    finally:
        service.close()

def legacy_main():
    """Legacy main function for backward compatibility"""
    from .services import process_company_filing
    
    tickers = ['AMZN']
    years = [2021, 2022, 2023, 2024]
    
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
    
    # Print legacy summary
    print("\n" + "=" * 50)
    print("LEGACY COMPATIBILITY RESULTS")
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
    print("SEC EDGAR Parser - Modern Architecture")
    print("=" * 50)
    
    # Run with modern architecture
    main()
    
    print("\n" + "=" * 50)
    print("Testing Legacy Compatibility")
    print("=" * 50)
    
    # Test legacy compatibility
    legacy_main()
