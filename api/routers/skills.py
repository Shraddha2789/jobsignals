"""GET /v1/skills — skill trends and demand signals."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.deps import get_db
from api.schemas.responses import APIResponse, Meta, SkillTrendOut
from pipeline.normalization.skill_extractor import SKILLS_TAXONOMY

router = APIRouter(prefix="/skills", tags=["Skills"])

VALID_WINDOWS = {7: "7d", 30: "30d", 90: "90d", 365: "365d"}


@router.get("/trends", response_model=APIResponse[list[SkillTrendOut]])
def skill_trends(
    title_family:    Optional[str] = Query(None),
    country:         Optional[str] = Query(None),
    window:          int           = Query(30, enum=[7, 30, 90, 365]),
    limit:           int           = Query(25, ge=1, le=100),
    order_by:        str           = Query("posting_share", enum=["posting_count", "posting_share", "mom_change"]),
    db:              Connection    = Depends(get_db),
):
    params: dict = {"window": window, "limit": limit}
    conditions = ["window_days = :window"]

    if title_family:
        conditions.append("title_family = :family")
        params["family"] = title_family

    if country:
        conditions.append("location_country = :country")
        params["country"] = country

    # Get the most recent period available
    latest_period = db.execute(
        text(
            f"SELECT MAX(period) FROM skill_trends WHERE {' AND '.join(conditions)}"
        ),
        params,
    ).scalar()

    if not latest_period:
        return APIResponse(
            data=[],
            meta=Meta(window=VALID_WINDOWS[window]),
        )

    conditions.append("period = :period")
    params["period"] = latest_period

    where = " AND ".join(conditions)

    if title_family:
        # Single-family query — use stored rows directly
        rows = db.execute(
            text(
                f"""
                SELECT skill_name, posting_count, posting_share, mom_change, yoy_change
                FROM skill_trends
                WHERE {where}
                ORDER BY {order_by} DESC NULLS LAST
                LIMIT :limit
                """
            ),
            params,
        ).fetchall()
    else:
        # Global query — aggregate across all families to avoid duplicate skill rows.
        # Recompute posting_share = skill_jobs / total_active_jobs.
        country_filter = "AND location_country = :country" if country else ""
        rows = db.execute(
            text(
                f"""
                WITH total AS (
                    SELECT COUNT(*) AS n FROM job_postings WHERE is_active = TRUE
                )
                SELECT st.skill_name,
                       SUM(st.posting_count)                              AS posting_count,
                       SUM(st.posting_count)::float / NULLIF(t.n, 0)     AS posting_share,
                       NULL::float                                        AS mom_change,
                       NULL::float                                        AS yoy_change
                FROM skill_trends st, total t
                WHERE st.window_days = :window
                  AND st.period      = :period
                  {country_filter}
                GROUP BY st.skill_name, t.n
                ORDER BY posting_count DESC NULLS LAST
                LIMIT :limit
                """
            ),
            params,
        ).fetchall()

    result = []
    for i, row in enumerate(rows):
        skill_name, count, share, mom, yoy = row
        # Look up category from taxonomy
        category = SKILLS_TAXONOMY.get(skill_name, {}).get("category")
        result.append(
            SkillTrendOut(
                skill_name=skill_name,
                skill_category=category,
                posting_count=count,
                posting_share=round(share, 4),
                mom_change=round(mom, 4) if mom is not None else None,
                yoy_change=round(yoy, 4) if yoy is not None else None,
                rank=i + 1,
            )
        )

    period_start = latest_period - timedelta(days=window)
    return APIResponse(
        data=result,
        meta=Meta(
            total_count=len(result),
            window=VALID_WINDOWS[window],
            period_start=str(period_start),
            period_end=str(latest_period),
        ),
    )


@router.get("/movers")
def skill_movers(
    limit:   int            = Query(8, ge=1, le=20),
    country: Optional[str] = Query(None, description="2-letter country code"),
    db:      Connection     = Depends(get_db),
):
    """
    Fastest rising and declining skills, computed as (7d daily rate) / (30d daily rate).
    momentum > 1.1 = rising; < 0.9 = declining.
    Requires both 7d and 30d aggregations to exist.
    """
    c = country.upper() if country else None
    params: dict = {}
    country_clause = ""
    if c:
        country_clause = "AND location_country = :country"
        params["country"] = c

    latest_7 = db.execute(
        text(f"SELECT MAX(period) FROM skill_trends WHERE window_days = 7 {country_clause}"),
        params,
    ).scalar()

    latest_30 = db.execute(
        text(f"SELECT MAX(period) FROM skill_trends WHERE window_days = 30 {country_clause}"),
        params,
    ).scalar()

    if not latest_7 or not latest_30:
        return {"data": {"rising": [], "declining": []}}

    params["period_7"]  = latest_7
    params["period_30"] = latest_30

    rows = db.execute(
        text(
            f"""
            WITH latest_7d AS (
                SELECT skill_name, SUM(posting_count) AS cnt_7
                FROM skill_trends
                WHERE window_days = 7 AND period = :period_7 {country_clause}
                GROUP BY skill_name
            ),
            latest_30d AS (
                SELECT skill_name, SUM(posting_count) AS cnt_30
                FROM skill_trends
                WHERE window_days = 30 AND period = :period_30 {country_clause}
                GROUP BY skill_name
            )
            SELECT l7.skill_name,
                   l7.cnt_7,
                   l30.cnt_30,
                   ROUND(
                     ((l7.cnt_7::float / 7.0) / NULLIF(l30.cnt_30::float / 30.0, 0))::numeric,
                     2
                   ) AS momentum
            FROM latest_7d l7
            JOIN latest_30d l30 ON l7.skill_name = l30.skill_name
            WHERE l30.cnt_30 >= 3
            ORDER BY momentum DESC
            """
        ),
        params,
    ).fetchall()

    def row_to_dict(r):
        return {
            "skill_name": r[0],
            "cnt_7d": int(r[1]),
            "cnt_30d": int(r[2]),
            "momentum": float(r[3]) if r[3] is not None else 1.0,
        }

    rising   = [row_to_dict(r) for r in rows          if r[3] and float(r[3]) > 1.1][:limit]
    declining = [row_to_dict(r) for r in reversed(rows) if r[3] and float(r[3]) < 0.9][:limit]

    return {"data": {"rising": rising, "declining": declining}}


@router.get("/taxonomy", response_model=APIResponse[dict])
def get_taxonomy():
    """Return the full skills taxonomy (canonical names + categories)."""
    summary = {
        name: {"category": meta["category"], "aliases": meta["aliases"]}
        for name, meta in SKILLS_TAXONOMY.items()
    }
    return APIResponse(data=summary, meta=Meta(total_count=len(summary)))
