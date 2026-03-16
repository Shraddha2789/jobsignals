"""GET /v1/stats — single-call KPI endpoint for the Overview dashboard page."""
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.deps import get_db

router = APIRouter(prefix="/stats", tags=["Stats"])


class StatsOut(BaseModel):
    total_postings:   int
    active_postings:  int
    unique_skills:    int
    companies_hiring: int
    role_families:    int
    last_ingested:    Optional[str]
    sources:          dict[str, int]
    top_family:       Optional[str]
    salary_coverage_pct: float
    modality_breakdown:  dict[str, int]


@router.get("", response_model=StatsOut)
def get_stats(
    country: Optional[str] = Query(None, description="2-letter country code, e.g. US, GB, IN, DE"),
    db: Connection = Depends(get_db),
):
    """Return all Overview KPIs in a single round-trip. Optionally filtered by country."""
    c = country.upper() if country else None
    p: dict = {"country": c} if c else {}

    def w(alias: str = "") -> str:
        """Build a WHERE fragment for the given table alias."""
        a = alias + "." if alias else ""
        base = f"{a}source_platform != 'seed'"
        if c:
            base += f" AND {a}location_country = :country"
        return base

    total = db.execute(
        text(f"SELECT COUNT(*) FROM job_postings WHERE {w()}"), p
    ).scalar() or 0

    active = db.execute(
        text(f"SELECT COUNT(*) FROM job_postings WHERE is_active = TRUE AND {w()}"), p
    ).scalar() or 0

    unique_skills = db.execute(
        text(
            f"SELECT COUNT(DISTINCT skill_name) "
            f"FROM job_skills js "
            f"JOIN job_postings jp ON jp.job_id = js.job_id "
            f"WHERE {w('jp')}"
        ),
        p,
    ).scalar() or 0

    companies = db.execute(
        text(
            f"SELECT COUNT(DISTINCT company_id) FROM job_postings "
            f"WHERE is_active = TRUE AND {w()} AND company_id IS NOT NULL"
        ),
        p,
    ).scalar() or 0

    families = db.execute(
        text(
            f"SELECT COUNT(DISTINCT title_family) FROM job_postings "
            f"WHERE {w()} AND title_family IS NOT NULL AND title_family != 'Other'"
        ),
        p,
    ).scalar() or 0

    last_row = db.execute(
        text(f"SELECT MAX(posted_at) FROM job_postings WHERE {w()}"), p
    ).scalar()
    last_ingested = last_row.isoformat() if last_row else None

    source_rows = db.execute(
        text(
            f"SELECT source_platform, COUNT(*) FROM job_postings "
            f"WHERE {w()} GROUP BY source_platform ORDER BY 2 DESC"
        ),
        p,
    ).fetchall()
    sources = {r[0]: r[1] for r in source_rows}

    top_family_row = db.execute(
        text(
            f"SELECT title_family, COUNT(*) AS cnt FROM job_postings "
            f"WHERE {w()} AND title_family IS NOT NULL AND title_family != 'Other' "
            f"GROUP BY title_family ORDER BY cnt DESC LIMIT 1"
        ),
        p,
    ).fetchone()
    top_family = top_family_row[0] if top_family_row else None

    salary_row = db.execute(
        text(f"SELECT COUNT(*) FROM job_postings WHERE {w()} AND salary_min IS NOT NULL"),
        p,
    ).scalar() or 0
    salary_pct = round(salary_row / total * 100, 1) if total > 0 else 0.0

    modality_rows = db.execute(
        text(
            f"""
            SELECT work_modality, COUNT(*)
            FROM job_postings
            WHERE {w()} AND is_active = TRUE
              AND work_modality IS NOT NULL AND work_modality != 'unspecified'
            GROUP BY work_modality ORDER BY 2 DESC
            """
        ),
        p,
    ).fetchall()
    modality_breakdown = {r[0]: r[1] for r in modality_rows}

    return StatsOut(
        total_postings=total,
        active_postings=active,
        unique_skills=unique_skills,
        companies_hiring=companies,
        role_families=families,
        last_ingested=last_ingested,
        sources=sources,
        top_family=top_family,
        salary_coverage_pct=salary_pct,
        modality_breakdown=modality_breakdown,
    )


@router.get("/history")
def stats_history(
    days:    int            = Query(90, ge=7, le=365, description="Lookback window in days"),
    country: Optional[str] = Query(None, description="2-letter country code"),
    db:      Connection     = Depends(get_db),
) -> dict[str, Any]:
    """Weekly posting counts for KPI sparklines. Returns up to ~13 weekly buckets."""
    c = country.upper() if country else None
    params: dict = {"days": days}
    country_clause = ""
    if c:
        country_clause = "AND location_country = :country"
        params["country"] = c

    rows = db.execute(
        text(
            f"""
            SELECT DATE_TRUNC('week', posted_at)::date AS week,
                   COUNT(*)                            AS postings
            FROM job_postings
            WHERE source_platform != 'seed'
              AND posted_at >= NOW() - INTERVAL '1 day' * :days
              {country_clause}
            GROUP BY 1
            ORDER BY 1
            """
        ),
        params,
    ).fetchall()

    return {"data": [{"week": str(r[0]), "postings": int(r[1])} for r in rows]}
