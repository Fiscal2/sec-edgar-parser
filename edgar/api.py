# app/api.py
from typing import List, Dict, Any, Optional
import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from starlette.concurrency import run_in_threadpool

from runner import run_multiple  # wraps process_company_filing for multi-year runs

# ---------- Logging ----------
logger = logging.getLogger("sec-edgar-api")
logging.basicConfig(level=logging.INFO)

# ---------- FastAPI app ----------
app = FastAPI(title="SEC Edgar Parser API", version="1.0.0")

# CORS (relax for dev; lock down in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],         # e.g. ["https://yourdomain.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Models ----------
class ParseRequest(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10, description="Ticker symbol, e.g. AAPL")
    years: List[int] = Field(..., description="One or more fiscal years, e.g. [2022, 2023]")

    @validator("ticker")
    def normalize_ticker(cls, v: str) -> str:
        return v.strip().upper()

    @validator("years")
    def validate_years(cls, v: List[int]) -> List[int]:
        if not v:
            raise ValueError("Must provide at least one year")
        # optional: sanity checks
        for y in v:
            if y < 1994 or y > 2100:
                raise ValueError(f"Year out of expected range: {y}")
        return v

class ParseResult(BaseModel):
    ticker: str
    attempted_years: List[int]
    results: Dict[str, Any]  # whatever run_multiple returns
    message: Optional[str] = None

# ---------- Routes ----------
@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.post("/parse", response_model=ParseResult)
async def parse_filings(payload: ParseRequest):
    """
    Kick off parsing for a ticker over multiple years.
    This will:
      - fetch filings from EDGAR
      - extract income/balance/cash
      - upload to Supabase via your uploader
    """
    try:
        logger.info("Received parse request: ticker=%s years=%s", payload.ticker, payload.years)
        # run the heavy CPU/network work off the event loop
        results = await run_in_threadpool(run_multiple, payload.ticker, payload.years)

        # You can normalize the shape here if needed
        return ParseResult(
            ticker=payload.ticker,
            attempted_years=payload.years,
            results=results,
            message="Completed parse job",
        )
    except Exception as e:
        logger.exception("Parse failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
