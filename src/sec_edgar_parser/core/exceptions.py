class EdgarParserException(Exception):
    """Base exception for all EDGAR parser errors"""
    pass

class FilingNotFoundException(EdgarParserException):
    """Raised when a filing cannot be found"""
    pass

class ParsingException(EdgarParserException):
    """Raised when parsing fails"""
    pass

class ValidationException(EdgarParserException):
    """Raised when data validation fails"""
    pass

class NetworkException(EdgarParserException):
    """Raised when network requests fail"""
    pass

class ConfigurationException(EdgarParserException):
    """Raised when configuration is invalid"""
    pass
