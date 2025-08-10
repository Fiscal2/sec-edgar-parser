# api/index.py
from fastapi import FastAPI
from edgar.api import router as edgar_router

app = FastAPI(title="EDGAR API")

@app.get("/")
def root():
    # This is what you’ll see at GET /api
    return {"status": "ok", "try": ["/api/health", "/api/docs", "/api/parse"]}

# include your real endpoints at the function root (no extra prefix)
app.include_router(edgar_router, prefix="")

