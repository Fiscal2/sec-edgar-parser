import logging
from typing import List, Dict, Any

from .main_service import MainService

logger = logging.getLogger(__name__)

class RunnerService:
    """Service for running the SEC filing parser"""
    
    def __init__(self):
        self.main_service = MainService()
    
    def run_multiple(self, ticker: str, years: List[int]) -> List[Dict[str, Any]]:
        """
        Run the parser for multiple years for the given ticker.
            
        Returns:
            List of dictionaries with year and success status
        """
        logger.info(f"Running parser for ticker={ticker}, years={years}")
        
        results = []
        for year in years:
            try:
                result = self.main_service.process_company_filing(ticker, year)
                results.append({
                    "year": year,
                    "success": result['success'],
                    "message": result.get('message', ''),
                    "error": result.get('error', '') if not result['success'] else None
                })
            except Exception as e:
                logger.exception(f"Error processing {ticker} for {year}: {e}")
                results.append({
                    "year": year,
                    "success": False,
                    "error": str(e),
                    "message": f"Unexpected error: {e}"
                })
        
        return results
    
    def run_batch(self, tickers: List[str], years: List[int]) -> Dict[str, Any]:
        """
        Run the parser for multiple tickers and years

        Returns:
            Dictionary containing batch processing results
        """
        logger.info(f"Running batch parser for tickers={tickers}, years={years}")
        
        try:
            results = self.main_service.process_multiple_companies(tickers, years)
            
            self._print_summary(results)
            
            return results
            
        except Exception as e:
            logger.exception(f"Error in batch processing: {e}")
            return {
                'successful': [],
                'failed': [f"{ticker} {year}" for ticker in tickers for year in years],
                'total_processed': len(tickers) * len(years),
                'error': str(e),
                'success_rate': 0.0
            }
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print a summary of the processing results"""
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
        
        success_rate = results.get('success_rate', 0.0)
        print(f"\nSuccess rate: {success_rate:.1f}%")
        
        if 'duration' in results:
            duration = results['duration']
            print(f"Total duration: {duration}")
    
    def close(self):
        """Close all service connections"""
        self.main_service.close()

# Backward compatibility function
def run_multiple(ticker: str, years: List[int]) -> List[Dict[str, Any]]:
    """Backward compatibility function for running multiple years"""
    runner = RunnerService()
    try:
        return runner.run_multiple(ticker, years)
    finally:
        runner.close()
