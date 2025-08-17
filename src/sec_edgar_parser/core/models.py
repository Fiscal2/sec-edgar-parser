from datetime import datetime
from typing import Dict, List, Optional, Union, Literal, Any
from pydantic import BaseModel, Field, field_validator, model_validator
from decimal import Decimal

class FinancialMetric(BaseModel):
    label: str
    value: Union[Decimal, str, None]
    namespace: Optional[str] = None
    
    @field_validator('value')
    @classmethod
    def validate_value(cls, v):
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v

class FinancialPeriod(BaseModel):
    date: datetime
    months: Optional[int] = None
    metrics: Dict[str, FinancialMetric] = Field(alias="map")
    
    class Config:
        populate_by_name = True

class FinancialStatement(BaseModel):
    company: str
    date_filed: datetime
    periods: List[FinancialPeriod] = Field(alias="reports")
    
    class Config:
        populate_by_name = True
    
    def add_period(self, period: FinancialPeriod) -> None:
        self.periods.append(period)
    
    @property
    def latest_period(self) -> Optional[FinancialPeriod]:
        return max(self.periods, key=lambda p: p.date) if self.periods else None

class FilingPeriod(BaseModel):
    type: Literal["annual", "quarterly"]
    year: int = Field(ge=1993, le=2100)
    quarter: Optional[int] = Field(None, ge=1, le=4)
    
    @model_validator(mode='after')
    def validate_quarterly_requires_quarter(self):
        if self.type == "quarterly" and self.quarter is None:
            raise ValueError('Quarter must be specified for quarterly filings')
        return self

class Company(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10)
    cik: str = Field(..., min_length=1, max_length=20)
    name: Optional[str] = None
    
    @field_validator('symbol')
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()

class FilingInfo(BaseModel):
    company: str
    form: str
    cik: str
    date_filed: datetime
    url: str
    file_path: str

class ParsedDocument(BaseModel):
    company: str
    filing_date: datetime
    form_type: str
    content: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)
