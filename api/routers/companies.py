"""GET /v1/companies — company lookup and hiring signals."""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.deps import get_db
from api.schemas.responses import (
    APIResponse, CompanyOut, CompanySignalsOut, HiringTrendPoint, Meta,
)

router = APIRouter(prefix="/companies", tags=["Companies"])


@router.get("", response_model=APIResponse[list[dict]])
def list_companies(
    q:             Optional[str] = Query(None, description="Company name search"),
    industry:      Optional[str] = Query(None),
    company_stage: Optional[str] = Query(None),
    country:       Optional[str] = Query(None),
    page_size:     int           = Query(50, ge=1, le=200),
    db:            Connection    = Depends(get_db),
):
    conditions = ["jp.source_platform != 'seed'", "jp.is_active = TRUE"]
    params: dict = {"page_size": page_size}

    if q:
        conditions.append("c.company_name ILIKE :q")
        params["q"] = f"%{q}%"
    if industry:
        conditions.append("c.industry ILIKE :industry")
        params["industry"] = f"%{industry}%"
    if company_stage:
        conditions.append("c.company_stage = :stage")
        params["stage"] = company_stage
    if country:
        # Filter by where the JOBS are posted, not where the company HQ is.
        # Most companies scraped from non-US sources have no hq_country set.
        conditions.append("jp.location_country = :country")
        params["country"] = country.upper()

    where = " AND ".join(conditions)

    rows = db.execute(
        text(f"""
            SELECT c.company_id, c.company_name, c.industry, c.company_stage,
                   c.employee_count_range, c.hq_country, c.domain,
                   COUNT(jp.job_id)                         AS active_jobs,
                   cs.top_skills, cs.top_roles,
                   cs.hiring_velocity_score
            FROM companies c
            JOIN job_postings jp ON jp.company_id = c.company_id
            LEFT JOIN LATERAL (
                SELECT top_skills, top_roles, hiring_velocity_score
                FROM company_signals
                WHERE company_id = c.company_id AND window_days = 90
                ORDER BY period DESC LIMIT 1
            ) cs ON TRUE
            WHERE {where}
            GROUP BY c.company_id, cs.top_skills, cs.top_roles, cs.hiring_velocity_score
            ORDER BY active_jobs DESC
            LIMIT :page_size
        """),
        params,
    ).mappings().fetchall()

    total = db.execute(
        text(f"""
            SELECT COUNT(DISTINCT c.company_id)
            FROM companies c
            JOIN job_postings jp ON jp.company_id = c.company_id
            WHERE {where}
        """),
        params,
    ).scalar()

    result = []
    for r in rows:
        r = dict(r)
        r["top_skills"] = r["top_skills"] if isinstance(r["top_skills"], list) else (json.loads(r["top_skills"]) if r["top_skills"] else [])
        r["top_roles"]  = r["top_roles"]  if isinstance(r["top_roles"],  list) else (json.loads(r["top_roles"])  if r["top_roles"]  else [])
        r["company_id"] = str(r["company_id"])
        result.append(r)

    return APIResponse(
        data=result,
        meta=Meta(total_count=total, page_size=page_size),
    )


@router.get("/{company_id}/signals", response_model=APIResponse[CompanySignalsOut])
def company_signals(
    company_id: UUID,
    window:     int        = Query(90, enum=[30, 90, 365]),
    db:         Connection = Depends(get_db),
):
    company = db.execute(
        text("SELECT * FROM companies WHERE company_id = :cid"),
        {"cid": str(company_id)},
    ).mappings().fetchone()

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    today = date.today()
    signals_row = db.execute(
        text(
            """
            SELECT total_postings, active_postings, hiring_velocity_score,
                   top_skills, top_roles, median_salary_min, median_salary_max
            FROM company_signals
            WHERE company_id = :cid AND window_days = :window
            ORDER BY period DESC
            LIMIT 1
            """
        ),
        {"cid": str(company_id), "window": window},
    ).fetchone()

    if not signals_row:
        # Fallback: compute from raw postings if signals not yet aggregated
        since = today - timedelta(days=window)
        raw_count = db.execute(
            text(
                "SELECT COUNT(*) FROM job_postings "
                "WHERE company_id = :cid AND posted_at >= :since"
            ),
            {"cid": str(company_id), "since": since},
        ).scalar() or 0
        total, active, velocity = raw_count, 0, 0.0
        top_skills, top_roles = [], []
        med_min = med_max = None
    else:
        total, active, velocity, top_skills_raw, top_roles_raw, med_min, med_max = signals_row
        top_skills = top_skills_raw if isinstance(top_skills_raw, list) else (json.loads(top_skills_raw) if top_skills_raw else [])
        top_roles  = top_roles_raw  if isinstance(top_roles_raw,  list) else (json.loads(top_roles_raw)  if top_roles_raw  else [])

    # Monthly trend for sparkline (last 6 months)
    trend_rows = db.execute(
        text(
            """
            SELECT DATE_TRUNC('month', posted_at)::date AS month,
                   COUNT(*) AS cnt
            FROM job_postings
            WHERE company_id = :cid
              AND posted_at >= NOW() - INTERVAL '6 months'
            GROUP BY 1
            ORDER BY 1
            """
        ),
        {"cid": str(company_id)},
    ).fetchall()
    trend = [HiringTrendPoint(period=str(r[0]), postings=r[1]) for r in trend_rows]

    salary_benchmarks = None
    if med_min or med_max:
        salary_benchmarks = {
            "median_min": med_min,
            "median_max": med_max,
            "currency": "USD",
        }

    return APIResponse(
        data=CompanySignalsOut(
            company_id=company_id,
            company_name=company["company_name"],
            window=f"{window}d",
            total_postings=total or 0,
            active_postings=active or 0,
            hiring_velocity_score=velocity,
            top_skills=top_skills,
            top_roles=top_roles,
            salary_benchmarks=salary_benchmarks,
            trend=trend,
        )
    )
