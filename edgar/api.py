# edgar/api.py
from typing import List, Dict, Any, Optional
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool

from edgar.runner import run_multiple

logger = logging.getLogger("sec-edgar-api")

router = APIRouter()

class ParseRequest(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10)
    years: List[int] = Field(...)

    @field_validator("ticker")
    def normalize_ticker(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("years")
    def validate_years(cls, v: List[int]) -> List[int]:
        if not v:
            raise ValueError("Must provide at least one year")
        for y in v:
            if y < 1994 or y > 2100:
                raise ValueError(f"Year out of expected range: {y}")
        return v

class ParseResult(BaseModel):
    ticker: str
    attempted_years: List[int]
    results: Dict[str, Any]
    message: Optional[str] = None

@router.get("/health", tags=["health"])
def health():
    return {"ok": True}

@router.post("/parse", response_model=ParseResult)  
async def parse_filings(payload: ParseRequest):
    try:
        results = await run_in_threadpool(run_multiple, payload.ticker, payload.years)
        return ParseResult(
            ticker=payload.ticker,
            attempted_years=payload.years,
            results=results,
            message="Completed parse job",
        )
    except Exception as e:
        logger.exception("Parse failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

