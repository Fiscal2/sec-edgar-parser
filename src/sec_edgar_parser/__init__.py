"""SEC EDGAR Parser - Extract financial data from SEC filings"""

__version__ = "0.1.0"
__author__ = "SEC EDGAR Parser Contributors"

from .core.models import (
    Company,
    FilingPeriod,
    FilingInfo,
    FinancialStatement,
    FinancialPeriod,
    FinancialMetric,
    ParsedDocument,
)

from .core.exceptions import (
    EdgarParserException,
    FilingNotFoundException,
    ParsingException,
    ValidationException,
    NetworkException,
    ConfigurationException,
)

from .parsers import (
    SgmlParser,
    DTD,
    Document,
    FinancialStatementParser,
)

from .services import (
    CompanyService,
    MainService,
    RunnerService,
)

from .api import (
    app,
    create_app,
    CompanyRequest,
    CompanyResponse,
    FilingRequest,
    FilingResponse,
    BatchRequest,
    BatchResponse,
)

__all__ = [
    "Company",
    "FilingPeriod",
    "FilingInfo",
    "FinancialStatement",
    "FinancialPeriod",
    "FinancialMetric",
    "ParsedDocument",
    "EdgarParserException",
    "FilingNotFoundException",
    "ParsingException",
    "ValidationException",
    "NetworkException",
    "ConfigurationException",
    "SgmlParser",
    "DTD",
    "Document",
    "FinancialStatementParser",
    "CompanyService",
    "MainService",
    "RunnerService",
    "app",
    "create_app",
    "CompanyRequest",
    "CompanyResponse",
    "FilingRequest",
    "FilingResponse",
    "BatchRequest",
    "BatchResponse",
]
