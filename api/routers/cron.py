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
    """Run all source adapters. Called by Vercel Cron every 6 hours."""
    _verify(authorization)
    try:
        from pipeline.runner import run_all_sources

        stats = run_all_sources()
        return {"status": "ok", **stats}
    except Exception as exc:
        log.error("Ingestion failed: %s", exc, exc_info=True)
        # Return 200 so Vercel does not send a failure email — error is in body.
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
