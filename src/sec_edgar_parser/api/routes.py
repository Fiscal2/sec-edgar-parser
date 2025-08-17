import time
import logging
from typing import List
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Request
from starlette.concurrency import run_in_threadpool

from .models import (
    CompanyRequest, CompanyResponse,
    FilingRequest, FilingResponse,
    BatchRequest, BatchResponse,
    HealthResponse, ErrorResponse
)
from ..services import CompanyService, MainService, RunnerService
from ..core.exceptions import (
    FilingNotFoundException, ValidationException, 
    ParsingException, NetworkException
)

logger = logging.getLogger(__name__)

router = APIRouter()


def get_company_service() -> CompanyService:
    """Dependency to get company service"""
    return CompanyService()


def get_main_service() -> MainService:
    """Dependency to get main service"""
    return MainService()


def get_runner_service() -> RunnerService:
    """Dependency to get runner service"""
    return RunnerService()


@router.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(),
        version="1.0.0",
        services={
            "company_service": "healthy",
            "main_service": "healthy",
            "parser": "healthy"
        }
    )


@router.get("/companies/{symbol}", response_model=CompanyResponse, tags=["Companies"])
async def get_company(
    symbol: str,
    company_service: CompanyService = Depends(get_company_service)
):
    """Get company information by symbol"""
    try:
        company = company_service.get_company(symbol)
        return CompanyResponse(
            symbol=company.symbol,
            cik=company.cik,
            name=company.name,
            message="Company found successfully"
        )
    except FilingNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting company {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/parse/filing", response_model=FilingResponse, tags=["Parser"])
async def parse_filing(
    request: FilingRequest,
    main_service: MainService = Depends(get_main_service)
):
    """Parse a single company filing"""
    start_time = time.time()
    
    try:
        result = await run_in_threadpool(
            main_service.process_company_filing,
            request.symbol,
            request.year
        )
        
        processing_time = int((time.time() - start_time) * 1000)
        
        return FilingResponse(
            success=result['success'],
            symbol=result['ticker'],
            year=result.get('target_year') or result.get('report_year') or result.get('year'),
            filing_year=result.get('filing_year'),
            company_name=result.get('company_name'),
            listed_exchange=result.get('listed_exchange'),
            form_type=result.get('form_type'),
            income_statement=result.get('income_statement'),
            balance_sheet=result.get('balance_sheet'),
            cash_flow=result.get('cash_flow'),
            message=result.get('message', ''),
            error=result.get('error'),
            processing_time_ms=processing_time
        )
        
    except FilingNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValidationException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ParsingException as e:
        raise HTTPException(status_code=422, detail=str(e))
    except NetworkException as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Error parsing filing for {request.symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/parse/batch", response_model=BatchResponse, tags=["Parser"])
async def parse_batch(
    request: BatchRequest,
    runner_service: RunnerService = Depends(get_runner_service)
):
    """Parse multiple companies and years in batch"""
    try:
        results = await run_in_threadpool(
            runner_service.run_batch,
            request.symbols,
            request.years
        )
        
        filing_responses = []
        for result in results.get('results', []):
            filing_responses.append(FilingResponse(
                success=result.get('success', False),
                symbol=result.get('ticker', ''),
                year=result.get('year', 0),
                filing_year=result.get('filing_year'),
                company_name=result.get('company_name'),
                listed_exchange=result.get('listed_exchange'),
                form_type=result.get('form_type'),
                income_statement=result.get('income_statement'),
                balance_sheet=result.get('balance_sheet'),
                cash_flow=result.get('cash_flow'),
                message=result.get('message', ''),
                error=result.get('error')
            ))
        
        return BatchResponse(
            total_processed=results['total_processed'],
            successful=results['successful'],
            failed=results['failed'],
            success_rate=results['success_rate'],
            duration_seconds=results['duration'].total_seconds() if 'duration' in results else 0.0,
            results=filing_responses
        )
        
    except ValidationException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in batch parsing: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/parse/{symbol}/{year}", response_model=FilingResponse, tags=["Parser"])
async def parse_filing_by_path(
    symbol: str,
    year: int,
    main_service: MainService = Depends(get_main_service)
):
    """Parse filing using path parameters (GET request)"""
    start_time = time.time()
    
    try:
        result = await run_in_threadpool(
            main_service.process_company_filing,
            symbol.upper(),
            year
        )
        
        processing_time = int((time.time() - start_time) * 1000)
        
        return FilingResponse(
            success=result['success'],
            symbol=result['ticker'],
            year=result.get('target_year') or result.get('report_year') or result.get('year'),
            filing_year=result.get('filing_year'),
            company_name=result.get('company_name'),
            listed_exchange=result.get('listed_exchange'),
            form_type=result.get('form_type'),
            income_statement=result.get('income_statement'),
            balance_sheet=result.get('balance_sheet'),
            cash_flow=result.get('cash_flow'),
            message=result.get('message', ''),
            error=result.get('error'),
            processing_time_ms=processing_time
        )
        
    except FilingNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValidationException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error parsing filing for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/companies", tags=["Companies"])
async def search_companies(
    query: str,
    limit: int = 10,
    company_service: CompanyService = Depends(get_company_service)
):
    """Search companies by symbol or name"""
    try:
        if len(query.strip()) < 2:
            raise HTTPException(status_code=400, detail="Search query must be at least 2 characters")
        
        companies = company_service.search_companies(query.strip(), limit)
        
        return {
            "query": query,
            "limit": limit,
            "results": [
                {
                    "symbol": company.symbol,
                    "cik": company.cik,
                    "name": company.name
                }
                for company in companies
            ]
        }
        
    except ValidationException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error searching companies: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
