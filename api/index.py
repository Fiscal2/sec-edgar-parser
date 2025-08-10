# edgar/api/index.py
from fastapi import FastAPI

# VERY minimal app to prove the function runs
app = FastAPI(title="EDGAR API")

@app.get("/")
def root():
    return {"status": "i'm ok mom"}  # /api should now return this

# Lazy import your real router so nothing heavy runs at import-time
try:
    from edgar.api import router as edgar_router  # put your endpoints in a router
    app.include_router(edgar_router, prefix="")
except Exception as e:
    # log import failures to function logs; don’t crash the cold start
    import traceback, sys
    print("Failed to import edgar.api router:", e, file=sys.stderr)
    traceback.print_exc()
