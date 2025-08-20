import json
import time
import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any
from src.sec_edgar_parser.core.supabase_client import get_supabase

logger = logging.getLogger(__name__)

class UploadService:
    def __init__(self, client=None):
        self.client = client or get_supabase()
    
    def _to_jsonable(self, obj):
        """
        Recursively convert obj into JSON-serializable types.
        Handles datetimes, Decimals, sets/tuples, and common custom object patterns
        (e.g., objects with .label / .value, or just .value).
        """
        # Primitives
        if obj is None or isinstance(obj, (str, int, float, bool)):
            return obj

        # Common numeric types
        if isinstance(obj, Decimal):
            return float(obj)

        # Datetime
        if isinstance(obj, datetime):
            return obj.isoformat()

        # Dict / Mapping
        if isinstance(obj, dict):
            # Ensure keys are strings; sanitize values
            return {str(k): self._to_jsonable(v) for k, v in obj.items()}

        # Iterables
        if isinstance(obj, (list, tuple, set)):
            return [self._to_jsonable(v) for v in obj]

        # Common "metric-like" objects
        # If it looks like it has a 'value', return that
        for attr in ("value", "amount", "val"):
            if hasattr(obj, attr):
                return self._to_jsonable(getattr(obj, attr))

        # If it has both label and value, return a small dict
        if hasattr(obj, "label") and hasattr(obj, "value"):
            try:
                return {
                    "label": self._to_jsonable(getattr(obj, "label")),
                    "value": self._to_jsonable(getattr(obj, "value")),
                }
            except Exception:
                # fall through to string fallback
                pass

        # Last resort: string representation
        return str(obj)

    def _strip_unwanted_keys(self, obj):
        """
        Remove keys we don't want stored inside the statement blobs.
        Currently drops 'company' wherever found.
        """
        if isinstance(obj, dict):
            obj.pop("company", None)
            for k, v in list(obj.items()):
                obj[k] = self._strip_unwanted_keys(v)
            return obj
        if isinstance(obj, list):
            return [self._strip_unwanted_keys(v) for v in obj]
        return obj

    def _clean_statement_blob(self, blob):
        """
        Strip unwanted keys, then convert everything to JSON-safe primitives.
        """
        # Make a shallow copy so we don't mutate caller refs
        safe = blob
        try:
            # strip keys
            safe = self._strip_unwanted_keys(safe)
            # convert to jsonable
            safe = self._to_jsonable(safe)
        except Exception as e:
            logger.warning(f"Sanitization warning: {e}")
        return safe

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

            income_clean  = self._clean_statement_blob(income)
            balance_clean = self._clean_statement_blob(balance)
            cash_clean    = self._clean_statement_blob(cash)

            # Validate data structure before JSON conversion
            try:
                json.dumps(income_clean)
                json.dumps(balance_clean)
                json.dumps(cash_clean)
            except (TypeError, ValueError) as e:
                logger.error(f"Data serialization error for {ticker} {year} Q{quarter}: {e}")
                raise ValueError(f"Data serialization error: {e}")

            # Prepare payload
            payload = {
                "ticker": ticker,
                "year": year,
                "quarter": quarter,
                "income_statement": json.dumps(income_clean),
                "balance_sheet": json.dumps(balance_clean),
                "cash_flow": json.dumps(cash_clean),
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
