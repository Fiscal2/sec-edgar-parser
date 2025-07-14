from supabase_client import supabase
import json
import time

def upload_to_supabase(ticker, year, quarter, income, balance, cash):
    try:
        # Check for duplicates
        existing = supabase.table("financials") \
            .select("id") \
            .eq("ticker", ticker) \
            .eq("year", year) \
            .eq("quarter", quarter) \
            .execute()

        if existing.data and len(existing.data) > 0:
            print(f"⏩ Skipping {ticker} Q{quarter} {year} — already in Supabase.")
            return

        # Convert FinancialReport objects to plain JSON-compatible dicts
        payload = {
            "ticker": ticker,
            "year": year,
            "quarter": quarter,
            "income_statement": json.dumps(income),
            "balance_sheet": json.dumps(balance),
            "cash_flow": json.dumps(cash),
        }

        # Upload to Supabase
        result = supabase.table("financials").insert(payload).execute()

        if result.data:
            print(f"✅ Uploaded {ticker} Q{quarter} {year}")
        else:
            print(f"❌ Upload failed: {result.error_message}")

        time.sleep(1 / 9)

    except Exception as e:
        print(f"❌ Error uploading {ticker} Q{quarter} {year}: {e}")