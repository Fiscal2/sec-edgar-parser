# api/index.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from edgar.api import router as edgar_router

# Add root_path so /api/index/* resolves to /health, /parse, etc. inside the app
app = FastAPI(title="EDGAR API", root_path="/api/index")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok", "try": ["/api/health", "/api/docs", "/api/parse"]}

app.include_router(edgar_router, prefix="")

# debug: list mounted routes so we can see what’s actually there
@app.get("/routes")
def routes():
    return sorted([r.path for r in app.router.routes])
