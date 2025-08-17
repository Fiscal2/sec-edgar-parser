import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi

from .routes import router
from ..core.exceptions import (
    EdgarParserException, FilingNotFoundException, 
    ValidationException, ParsingException, NetworkException
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting SEC EDGAR Parser API...")
    yield
    logger.info("Shutting down SEC EDGAR Parser API...")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    app = FastAPI(
        title="SEC EDGAR Parser API",
        description="API for parsing SEC EDGAR financial filings",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include API routes
    app.include_router(router, prefix="/api/v1")
    
    # Root endpoints
    @app.get("/", tags=["Root"])
    async def root():
        """Root endpoint with API information"""
        return {
            "name": "SEC EDGAR Parser API",
            "version": "1.0.0",
            "description": "API for parsing SEC EDGAR financial filings",
            "endpoints": {
                "health": "/api/v1/health",
                "companies": "/api/v1/companies",
                "parse": "/api/v1/parse",
                "docs": "/docs",
                "redoc": "/redoc"
            },
            "timestamp": datetime.now().isoformat()
        }

    @app.get("/health", tags=["Health"])
    async def health_check():
        """Root level health check"""
        return {
            "status": "healthy",
            "service": "SEC EDGAR Parser API",
            "timestamp": datetime.now().isoformat()
        }
    
    @app.exception_handler(EdgarParserException)
    async def edgar_exception_handler(request: Request, exc: EdgarParserException):
        return JSONResponse(
            status_code=500,
            content={
                "error": "EDGAR Parser Error",
                "message": str(exc),
                "timestamp": datetime.now().isoformat(),
                "path": str(request.url)
            }
        )
    
    @app.exception_handler(FilingNotFoundException)
    async def filing_not_found_handler(request: Request, exc: FilingNotFoundException):
        return JSONResponse(
            status_code=404,
            content={
                "error": "Filing Not Found",
                "message": str(exc),
                "timestamp": datetime.now().isoformat(),
                "path": str(request.url)
            }
        )
    
    @app.exception_handler(ValidationException)
    async def validation_exception_handler(request: Request, exc: ValidationException):
        return JSONResponse(
            status_code=400,
            content={
                "error": "Validation Error",
                "message": str(exc),
                "timestamp": datetime.now().isoformat(),
                "path": str(request.url)
            }
        )
    
    @app.exception_handler(ParsingException)
    async def parsing_exception_handler(request: Request, exc: ParsingException):
        return JSONResponse(
            status_code=422,
            content={
                "error": "Parsing Error",
                "message": str(exc),
                "timestamp": datetime.now().isoformat(),
                "path": str(request.url)
            }
        )
    
    @app.exception_handler(NetworkException)
    async def network_exception_handler(request: Request, exc: NetworkException):
        return JSONResponse(
            status_code=503,
            content={
                "error": "Network Error",
                "message": str(exc),
                "timestamp": datetime.now().isoformat(),
                "path": str(request.url)
            }
        )
    
    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        
        openapi_schema = get_openapi(
            title="SEC EDGAR Parser API",
            version="1.0.0",
            description="API for parsing SEC EDGAR financial filings",
            routes=app.routes,
        )
        
        openapi_schema["info"]["x-logo"] = {
            "url": "https://www.sec.gov/favicon.ico"
        }
        
        app.openapi_schema = openapi_schema
        return app.openapi_schema
    
    app.openapi = custom_openapi
    
    return app


app = create_app()
