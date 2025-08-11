from typing import List, Dict, Any, Optional
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool

from edgar.runner import run_multiple

logger = logging.getLogger("sec-edgar-api")

# Explicitly set no prefix — index.py handles root_path="/api"
router = APIRouter()

# -------------------------
# Request/Response Models
# -------------------------

class ParseRequest(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10, description="Stock ticker symbol (e.g., COST)")
    years: List[int] = Field(..., description="List of years to parse")

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
    results: List[Dict[str, Any]]
    message: Optional[str] = None

# -------------------------
# Routes
# -------------------------

@router.get("/health", tags=["Health"])
def health():
    """Health check endpoint for the EDGAR parser API."""
    return {"ok": True}

@router.post("/parse", response_model=ParseResult, tags=["Parser"])
async def parse_filings(payload: ParseRequest):
    """
    Run the EDGAR parser for the given ticker and years.

    - **ticker**: Stock ticker symbol (e.g., COST)
    - **years**: List of years to parse (1994-2100)
    """
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
        return ParseResult(
            ticker=payload.ticker,
            attempted_years=payload.years,
            results=[{"year": y, "success": False, "error": str(e)} for y in payload.years],
            message="Parse failed",
        )


