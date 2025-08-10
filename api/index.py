# api/index.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from edgar.api import router as edgar_router

app = FastAPI(title="EDGAR API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "ok", "try": ["/api/health", "/api/parse", "/api/docs"]}


@app.get("/health")
def health_top():
    return {"ok": True, "source": "top-level"}

app.include_router(edgar_router, prefix="")
    
