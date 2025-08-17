import logging
import pandas as pd
from pathlib import Path
from typing import Optional, List

from ..core.models import Company
from ..core.exceptions import FilingNotFoundException, ValidationException

logger = logging.getLogger(__name__)

class CompanyService:
    def __init__(self, symbols_data_path: Optional[str] = None):
        self.symbols_data_path = symbols_data_path or self._get_default_symbols_path()
        self._symbols_cache: Optional[pd.DataFrame] = None
    
    def _get_default_symbols_path(self) -> str:
        """Get the default path to the symbols CSV file"""
        current_dir = Path(__file__).parent.parent.parent.parent
        return str(current_dir / "edgar" / "data" / "symbols.csv")
    
    def _load_symbols_data(self) -> pd.DataFrame:
        """Load and cache the symbols data"""
        if self._symbols_cache is not None:
            return self._symbols_cache
        
        try:
            df = pd.read_csv(self.symbols_data_path, converters={'cik': str})
            self._symbols_cache = df
            logger.debug(f"Loaded {len(df)} company symbols from {self.symbols_data_path}")
            return df
        except FileNotFoundError:
            raise ValidationException(f"Symbols data file not found: {self.symbols_data_path}")
        except Exception as e:
            raise ValidationException(f"Failed to load symbols data: {e}")
    
    def get_company(self, symbol: str) -> Company:
        """Get company information by symbol"""
        if not symbol:
            raise ValidationException("Symbol cannot be empty")
        
        symbol = symbol.strip().upper()
        df = self._load_symbols_data()
        
        try:
            company_data = df.loc[df['symbol'] == symbol].iloc[0]
            cik = company_data['cik']
            name = company_data.get('name', None)
            
            logger.info(f"Found company {symbol} with CIK {cik}")
            return Company(symbol=symbol, cik=cik, name=name)
            
        except IndexError:
            raise FilingNotFoundException(f"Company with symbol '{symbol}' not found in symbols database")
    
    def get_companies_by_cik(self, cik: str) -> List[Company]:
        """Get companies by CIK (multiple companies can share a CIK)"""
        if not cik:
            raise ValidationException("CIK cannot be empty")
        
        df = self._load_symbols_data()
        matches = df[df['cik'] == cik]
        
        if matches.empty:
            raise FilingNotFoundException(f"No companies found with CIK {cik}")
        
        companies = []
        for _, row in matches.iterrows():
            companies.append(Company(
                symbol=row['symbol'],
                cik=row['cik'],
                name=row.get('name', None)
            ))
        
        return companies
    
    def search_companies(self, query: str, limit: int = 10) -> List[Company]:
        """Search companies by symbol or name"""
        if not query or len(query.strip()) < 2:
            raise ValidationException("Search query must be at least 2 characters")
        
        df = self._load_symbols_data()
        query = query.strip().lower()
        
        symbol_matches = df[df['symbol'].str.lower().str.contains(query, na=False)]
        name_matches = df[df['name'].str.lower().str.contains(query, na=False)] if 'name' in df.columns else pd.DataFrame()
        
        combined = pd.concat([symbol_matches, name_matches]).drop_duplicates(subset=['symbol'])
        combined = combined.head(limit)
        
        companies = []
        for _, row in combined.iterrows():
            companies.append(Company(
                symbol=row['symbol'],
                cik=row['cik'],
                name=row.get('name', None)
            ))
        
        return companies
    
    def refresh_symbols_cache(self) -> None:
        """Clear the symbols cache"""
        self._symbols_cache = None
        logger.debug("Symbols cache cleared")
