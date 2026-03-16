"""GET /v1/salaries — salary benchmarks by role, seniority, and location."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.deps import get_db
from api.schemas.responses import APIResponse, SalaryBenchmarkOut

router = APIRouter(prefix="/salaries", tags=["Salaries"])

TITLE_FAMILIES = [
    "Data Engineering", "Data Science", "ML Engineering",
    "Software Engineering", "Product Management",
]
SENIORITY_LEVELS = ["intern", "junior", "mid", "senior", "staff", "principal", "executive"]


@router.get("/benchmark", response_model=APIResponse[SalaryBenchmarkOut])
def salary_benchmark(
    title_family:  str           = Query(..., description="Role family"),
    seniority:     str           = Query(..., description="Seniority level"),
    country:       str           = Query("US"),
    company_stage: Optional[str] = Query(None),
    window_days:   int           = Query(90, enum=[30, 90, 365]),
    db:            Connection    = Depends(get_db),
):
    if title_family not in TITLE_FAMILIES:
        raise HTTPException(
            status_code=422,
            detail=f"title_family must be one of: {', '.join(TITLE_FAMILIES)}",
        )
    if seniority not in SENIORITY_LEVELS:
        raise HTTPException(
            status_code=422,
            detail=f"seniority must be one of: {', '.join(SENIORITY_LEVELS)}",
        )

    since = date.today() - timedelta(days=window_days)
    params: dict = {
        "family":   title_family,
        "seniority": seniority,
        "country":  country.upper(),
        "since":    since,
    }

    extra_join  = ""
    extra_where = ""
    if company_stage:
        extra_join  = "JOIN companies c ON c.company_id = jp.company_id"
        extra_where = "AND c.company_stage = :stage"
        params["stage"] = company_stage

    row = db.execute(
        text(
            f"""
            SELECT
                COUNT(*)                                                          AS sample_size,
                PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY salary_min)::int    AS p10,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_min)::int    AS p25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_min)::int    AS p50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_max)::int    AS p75,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY salary_max)::int    AS p90
            FROM job_postings jp
            {extra_join}
            WHERE jp.title_family    = :family
              AND jp.seniority_level = :seniority
              AND jp.location_country = :country
              AND jp.salary_min IS NOT NULL
              AND jp.posted_at >= :since
              {extra_where}
            """
        ),
        params,
    ).fetchone()

    sample_size = row[0] if row else 0

    if sample_size < 10:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Insufficient data: only {sample_size} postings with salary for "
                f"{title_family} / {seniority} in {country}. Minimum 10 required."
            ),
        )

    return APIResponse(
        data=SalaryBenchmarkOut(
            title_family=title_family,
            seniority=seniority,
            country=country.upper(),
            percentile_10=row[1],
            percentile_25=row[2],
            percentile_50=row[3],
            percentile_75=row[4],
            percentile_90=row[5],
            currency="USD",
            sample_size=sample_size,
            period=f"{window_days}d",
        )
    )
