import json
from typing import Optional, Dict, Any
from src.sec_edgar_parser.core.supabase_client import get_supabase

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
        listed_exchange: Optional[Dict[str, Any]] = None
    ):
        payload = {
            "ticker": ticker,
            "year": year,
            "quarter": quarter,
            "income_statement": json.dumps(income),
            "balance_sheet": json.dumps(balance),
            "cash_flow": json.dumps(cash),
            "company_name": company_name,
        }
        
        if listed_exchange:
            payload["listed_exchange"] = json.dumps(listed_exchange)
        
        # Use upsert with a composite unique key if you have one
        return (
            self.client.table("financials")
            .upsert(payload, on_conflict=["ticker","year","quarter"])
            .execute()
        )
