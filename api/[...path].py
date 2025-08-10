# api/index.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from edgar.api import router as edgar_router

app = FastAPI(title="EDGAR API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok", "try": ["/api/index/health", "/api/index/docs", "/api/index/parse"]}

# Mount routes at BOTH "" and "/index" to handle Vercel pathing
app.include_router(edgar_router, prefix="")         # matches /api/index/health when ASGI path == "/health"
app.include_router(edgar_router, prefix="/index")   # matches /api/index/health when ASGI path == "/index/health"

# (optional) temporary debug route
@app.get("/__scope")
def scope(request: Request):
    return {
        "root_path": request.scope.get("root_path"),
        "path": request.scope.get("path"),
        "headers": [(k.decode(), v.decode()) for k, v in request.scope.get("headers", [])]
    }
