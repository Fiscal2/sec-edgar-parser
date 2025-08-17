from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class CompanyRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10, description="Stock ticker symbol")
    
    @field_validator("symbol")
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()


class FilingRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10, description="Stock ticker symbol")
    year: int = Field(..., ge=1993, le=2100, description="Target year for filing")
    period: str = Field(default="annual", pattern="^(annual|quarterly)$", description="Filing period type")
    quarter: Optional[int] = Field(None, ge=1, le=4, description="Quarter (required for quarterly filings)")
    
    @field_validator("symbol")
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()
    
    @field_validator("quarter")
    def validate_quarterly_quarter(cls, v: Optional[int], info):
        if info.data.get("period") == "quarterly" and v is None:
            raise ValueError("Quarter must be specified for quarterly filings")
        return v


class BatchRequest(BaseModel):
    symbols: List[str] = Field(..., min_items=1, max_items=100, description="List of stock ticker symbols")
    years: List[int] = Field(..., min_items=1, description="List of years to process")
    period: str = Field(default="annual", pattern="^(annual|quarterly)$", description="Filing period type")
    
    @field_validator("symbols")
    def normalize_symbols(cls, v: List[str]) -> List[str]:
        return [s.strip().upper() for s in v if s.strip()]
    
    @field_validator("years")
    def validate_years(cls, v: List[int]) -> List[int]:
        for year in v:
            if year < 1993 or year > 2100:
                raise ValueError(f"Year {year} out of valid range (1993-2100)")
        return sorted(list(set(v)))  # Remove duplicates and sort


class CompanyResponse(BaseModel):
    symbol: str
    cik: str
    name: Optional[str]
    message: str = "Success"


class FilingResponse(BaseModel):
    success: bool
    symbol: str
    year: int
    filing_year: Optional[int]
    company_name: Optional[str]
    listed_exchange: Optional[str]
    form_type: Optional[str]
    income_statement: Optional[Dict[str, Any]]
    balance_sheet: Optional[Dict[str, Any]]
    cash_flow: Optional[Dict[str, Any]]
    message: str
    error: Optional[str] = None
    processing_time_ms: Optional[int] = None


class BatchResponse(BaseModel):
    total_processed: int
    successful: List[str]
    failed: List[str]
    success_rate: float
    duration_seconds: float
    results: List[FilingResponse]


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    services: Dict[str, str]


class ErrorResponse(BaseModel):
    error: str
    message: str
    timestamp: datetime
    request_id: Optional[str] = None
