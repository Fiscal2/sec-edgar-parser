#!/usr/bin/env python3
"""
Server for running the SEC EDGAR Parser API.
"""

import uvicorn
from .api.main import create_app

if __name__ == "__main__":
    app = create_app()
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=True
    )
