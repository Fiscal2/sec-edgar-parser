from edgar.stock import Stock
from uploader import upload_to_supabase
from datetime import datetime

def convert_report(report):
    def convert(obj):
        if isinstance(obj, list):
            return [convert(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, datetime):
            return obj.strftime("%d-%m-%Y")  # Format: DD-MM-YYYY
        elif hasattr(obj, "__dict__"):
            return convert(obj.__dict__)
        else:
            return obj
        
    return convert(report)


# --- Configuration ---
tickers = ['AAPL', 'MSFT', 'GOOG']   # Add more tickers if needed
years = range(2022, 2023)             # Adjust year range as needed

# --- Loop through companies and years ---
for ticker in tickers:
    print(f"\nStarting annual filings for {ticker}...")
    stock = Stock(ticker)

    for year in years:
        print(f"\nProcessing {ticker} 10-K for {year}...")
        try:
            filing = stock.get_filing('annual', year, 4)
            if not filing:
                print(f"No 10-K found for {ticker} {year}")
                continue

            income = convert_report(filing.get_income_statements())
            balance = convert_report(filing.get_balance_sheets())
            cash = convert_report(filing.get_cash_flows())

            # quarter = 0 is our convention for annual
            
            if income and balance and cash:
                upload_to_supabase(ticker, year, 0, income, balance, cash)
            else:
                print(f"⚠️  Skipping upload for {ticker} {year} — missing financial data")

        except Exception as e:
            print(f"Failed to process {ticker} 10-K {year}: {e}")