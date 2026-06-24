"""
Manual trigger endpoints for ingestion and aggregation.

Scheduling is handled by GitHub Actions (.github/workflows/ingest.yml).
These endpoints exist for ad-hoc manual runs — hit them from curl or the
API explorer without needing to log into GitHub.

Protected by CRON_SECRET if set; open locally when the env var is absent.
Always return HTTP 200 — errors are in the response body, not the status code.
"""

# from __future__ import annotations needed so `str | None` resolves on Python 3.9
from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Header, HTTPException

log = logging.getLogger("cron")

router = APIRouter(prefix="/cron", tags=["Cron"])

_CRON_SECRET = os.environ.get("CRON_SECRET", "")


def _verify(authorization: str | None) -> None:
    if _CRON_SECRET and authorization != f"Bearer {_CRON_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")


@router.get("/ingest")
def cron_ingest(authorization: str | None = Header(default=None)):
    """Run all source adapters (dev/local use). For Vercel use the /ingest/* split endpoints."""
    _verify(authorization)
    try:
        from pipeline.runner import run_all_sources

        stats = run_all_sources()
        return {"status": "ok", **stats}
    except Exception as exc:
        log.error("Ingestion failed: %s", exc, exc_info=True)
        return {"status": "error", "detail": str(exc)}


@router.get("/ingest/free")
def cron_ingest_free(authorization: str | None = Header(default=None)):
    """Free sources: RemoteOK, Remotive, Arbeitnow, Himalayas, Jobicy, TheMuse, WeWorkRemotely."""
    _verify(authorization)
    try:
        from pipeline.runner import run_free_sources

        stats = run_free_sources()
        return {"status": "ok", **stats}
    except Exception as exc:
        log.error("Ingestion/free failed: %s", exc, exc_info=True)
        return {"status": "error", "detail": str(exc)}


@router.get("/ingest/paid")
def cron_ingest_paid(authorization: str | None = Header(default=None)):
    """Paid API sources: Adzuna (6 countries) + Jooble."""
    _verify(authorization)
    import os

    adzuna_configured = bool(os.environ.get("ADZUNA_APP_ID") and os.environ.get("ADZUNA_APP_KEY"))
    try:
        from pipeline.runner import run_paid_sources

        stats = run_paid_sources()
        return {"status": "ok", "adzuna_configured": adzuna_configured, **stats}
    except Exception as exc:
        log.error("Ingestion/paid failed: %s", exc, exc_info=True)
        return {"status": "error", "adzuna_configured": adzuna_configured, "detail": str(exc)}


@router.get("/ingest/scrape")
def cron_ingest_scrape(authorization: str | None = Header(default=None)):
    """Scraper sources: SerpAPI (Google Jobs) + Apify LinkedIn + Apify Indeed."""
    _verify(authorization)
    try:
        from pipeline.runner import run_scrape_sources

        stats = run_scrape_sources()
        return {"status": "ok", **stats}
    except Exception as exc:
        log.error("Ingestion/scrape failed: %s", exc, exc_info=True)
        return {"status": "error", "detail": str(exc)}


@router.get("/aggregate")
def cron_aggregate(authorization: str | None = Header(default=None)):
    """Refresh skill_trends and company_signals. Called by Vercel Cron once a day."""
    _verify(authorization)
    try:
        from pipeline.aggregations import run_all_aggregations

        result = run_all_aggregations()
        return {"status": "ok", "result": str(result)}
    except Exception as exc:
        log.error("Aggregation failed: %s", exc, exc_info=True)
        return {"status": "error", "detail": str(exc)}
