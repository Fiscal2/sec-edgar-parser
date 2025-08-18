import json
import time
import logging
from typing import Optional, Dict, Any
from src.sec_edgar_parser.core.supabase_client import get_supabase

logger = logging.getLogger(__name__)

class UploadService:
    def __init__(self, client=None):
        self.client = client or get_supabase()

    def upsert_financials(
        self,
        ticker: str,
        year: int,
        quarter: int,
        income: Dict[str, Any],
        balance: Dict[str, Any],
        cash: Dict[str, Any],
        company_name: Optional[str] = None,
        listed_exchange: Optional[Dict[str, Any]] = None,
        total_revenue: Optional[float] = None
    ):
        """Upload financial data to Supabase with duplicate checking and validation."""
        try:
            # Check for duplicates
            logger.info(f"Checking for existing data: {ticker} {year} Q{quarter}")
            existing = self.client.table("financials") \
                .select("id, company_name, listed_exchange") \
                .eq("ticker", ticker) \
                .eq("year", year) \
                .eq("quarter", quarter) \
                .execute()
            
            if existing.data and len(existing.data) > 0:
                row = existing.data[0]
                existing_company_name = row.get("company_name")
                existing_listed_exchange = row.get("listed_exchange")
                
                # Skip only if both are already present
                if existing_company_name and existing_listed_exchange:
                    logger.info(f"Skipping {ticker} Q{quarter} {year} — already in Supabase with company name and listed_exchange.")
                    return existing
                else:
                    logger.info("Existing record missing one or more fields — will update via upsert.")

            # Validate data structure before JSON conversion
            try:
                json.dumps(income)
                json.dumps(balance)
                json.dumps(cash)
            except (TypeError, ValueError) as e:
                logger.error(f"Data serialization error for {ticker} {year} Q{quarter}: {e}")
                raise ValueError(f"Data serialization error: {e}")

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
                payload["listed_exchange"] = json.dumps(listed_exchange)

            if total_revenue is not None:
                payload["total_revenue"] = total_revenue

            # Upload to Supabase
            logger.info(f"Uploading {ticker} Q{quarter} {year} to Supabase...")
            result = self.client.table("financials").upsert(payload, on_conflict="ticker,year,quarter").execute()
            
            if result.data:
                logger.info(f"✅ Successfully uploaded {ticker} Q{quarter} {year}")
                time.sleep(1 / 9)  # Rate limiting
                return result
            else:
                error_msg = getattr(result, 'error_message', 'Unknown error')
                logger.error(f"❌ Upload failed for {ticker} Q{quarter} {year}: {error_msg}")
                raise Exception(f"Upload failed: {error_msg}")
                
        except Exception as e:
            logger.error(f"❌ Error uploading {ticker} Q{quarter} {year}: {e}")
            raise

    def get_existing_data(self, ticker: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Get existing financial data from Supabase."""
        try:
            result = self.client.table("financials") \
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
