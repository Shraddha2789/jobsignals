"""FastAPI application entry point."""
from __future__ import annotations

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import text

from api.routers import v1_router
from db import check_connection, get_connection

app = FastAPI(
    title="JobSignals API",
    description="Job market intelligence — canonical dataset for hiring trends, skill demand, and salary signals.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(v1_router)


@app.on_event("startup")
def init_db_schema():
    """Apply schema on startup. Safe to run repeatedly — uses CREATE TABLE IF NOT EXISTS."""
    schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
    if schema_path.exists():
        with get_connection() as conn:
            conn.execute(text(schema_path.read_text()))


@app.get("/", include_in_schema=False)
def dashboard():
    """Serve the JobSignals dashboard."""
    return FileResponse(Path(__file__).parent.parent / "dashboard" / "index.html")


@app.get("/health", tags=["System"])
def health():
    db_ok = check_connection()
    return {
        "status": "ok" if db_ok else "degraded",
        "database": "connected" if db_ok else "unreachable",
        "version": "1.0.0",
    }



def start():
    uvicorn.run(
        "api.main:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),
        port=int(os.environ.get("API_PORT", 8000)),
        reload=os.environ.get("API_RELOAD", "true").lower() == "true",
        log_level=os.environ.get("API_LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    start()
