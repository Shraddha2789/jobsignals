"""GET /v1/jobs — job search and retrieval."""
from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.deps import get_db
from api.schemas.responses import APIResponse, CompanyOut, JobOut, LocationOut, Meta, SalaryOut

router = APIRouter(prefix="/jobs", tags=["Jobs"])


def _build_job(row: dict, skills: list[str]) -> JobOut:
    company = None
    if row["company_id"]:
        company = CompanyOut(
            company_id=row["company_id"],
            company_name=row["company_name"] or "",
            domain=row["domain"],
            industry=row["industry"],
            company_stage=row["company_stage"],
            employee_count_range=row["employee_count_range"],
            hq_country=row["hq_country"],
        )
    salary = None
    if row["salary_min"] or row["salary_max"]:
        salary = SalaryOut(
            min=row["salary_min"],
            max=row["salary_max"],
            currency=row["salary_currency"] or "USD",
            source=row["salary_source"],
        )
    return JobOut(
        job_id=row["job_id"],
        source_platform=row["source_platform"],
        source_url=row["source_url"],
        title_raw=row["title_raw"],
        title_normalized=row["title_normalized"],
        title_family=row["title_family"],
        seniority_level=row["seniority_level"],
        company=company,
        location=LocationOut(
            raw=row["location_raw"],
            city=row["location_city"],
            country=row["location_country"],
            modality=row["work_modality"] or "unspecified",
        ),
        employment_type=row["employment_type"] or "full_time",
        salary=salary,
        skills=skills,
        posted_at=row["posted_at"],
        is_active=row["is_active"],
    )


def _encode_cursor(job_id: str, posted_at: datetime) -> str:
    payload = json.dumps({"job_id": str(job_id), "posted_at": str(posted_at)})
    return base64.urlsafe_b64encode(payload.encode()).decode()


def _decode_cursor(cursor: str) -> dict:
    payload = base64.urlsafe_b64decode(cursor.encode()).decode()
    return json.loads(payload)


@router.get("", response_model=APIResponse[list[JobOut]])
def list_jobs(
    q:            Optional[str]  = Query(None, description="Full-text search on title + description"),
    title_family: Optional[str]  = Query(None),
    company_id:   Optional[UUID] = Query(None),
    location:     Optional[str]  = Query(None, description="City free-text search"),
    country:      Optional[str]  = Query(None, description="Exact 2-letter country code, e.g. US, GB, IN"),
    modality:     Optional[str]  = Query(None, enum=["remote", "hybrid", "onsite"]),
    seniority:    Optional[str]  = Query(None),
    salary_min:   Optional[int]  = Query(None),
    posted_after: Optional[str]  = Query(None, description="ISO 8601 date"),
    skills:       Optional[str]  = Query(None, description="Comma-separated skill names"),
    page_size:    int             = Query(20, ge=1, le=100),
    offset:       int             = Query(0, ge=0),
    cursor:       Optional[str]  = Query(None),
    sort:         Optional[str]  = Query(None, description="newest|oldest|salary_desc|salary_asc"),
    db:           Connection      = Depends(get_db),
):
    conditions = ["jp.is_active = TRUE", "jp.source_platform != 'seed'"]
    params: dict = {"page_size": page_size, "offset": offset}

    if title_family:
        conditions.append("jp.title_family = :title_family")
        params["title_family"] = title_family

    if company_id:
        conditions.append("jp.company_id = :company_id")
        params["company_id"] = str(company_id)

    if modality:
        conditions.append("jp.work_modality = :modality")
        params["modality"] = modality

    if seniority:
        conditions.append("jp.seniority_level = :seniority")
        params["seniority"] = seniority

    if salary_min:
        conditions.append("jp.salary_min >= :salary_min")
        params["salary_min"] = salary_min

    if posted_after:
        conditions.append("jp.posted_at >= :posted_after")
        params["posted_after"] = posted_after

    if country:
        # Exact 2-letter country code match (used by global country selector)
        conditions.append("jp.location_country = :country_code")
        params["country_code"] = country.upper()

    if location:
        # Free-text city search (used by city search field, not country codes)
        conditions.append("jp.location_city ILIKE :loc")
        params["loc"] = f"%{location}%"

    if q:
        conditions.append(
            "(jp.title_raw ILIKE :q OR jp.description_cleaned ILIKE :q)"
        )
        params["q"] = f"%{q}%"

    if cursor:
        try:
            cur_data = _decode_cursor(cursor)
            conditions.append(
                "(jp.posted_at, jp.job_id::text) < (:cursor_posted_at, :cursor_job_id)"
            )
            params["cursor_posted_at"] = cur_data["posted_at"]
            params["cursor_job_id"]    = cur_data["job_id"]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor")

    where = " AND ".join(conditions)

    # Skills filter — jobs must have ALL listed skills
    skills_join = ""
    if skills:
        skill_list = [s.strip() for s in skills.split(",") if s.strip()]
        for idx, skill in enumerate(skill_list):
            alias = f"sk{idx}"
            skills_join += (
                f" JOIN job_skills {alias} ON {alias}.job_id = jp.job_id "
                f"AND {alias}.skill_name = :skill_{idx}"
            )
            params[f"skill_{idx}"] = skill

    # Count total (without cursor for pagination display)
    count_where = " AND ".join([c for c in conditions if "cursor" not in c])
    total = db.execute(
        text(f"SELECT COUNT(*) FROM job_postings jp {skills_join} WHERE {count_where}"),
        params,
    ).scalar()

    order_clause = {
        "oldest":      "jp.posted_at ASC,  jp.job_id ASC",
        "salary_desc": "jp.salary_min DESC NULLS LAST, jp.posted_at DESC",
        "salary_asc":  "jp.salary_min ASC  NULLS LAST, jp.posted_at DESC",
    }.get(sort or "", "jp.posted_at DESC, jp.job_id DESC")

    query = text(
        f"""
        SELECT jp.*,
               c.company_name, c.domain, c.industry, c.company_stage,
               c.employee_count_range, c.hq_country
        FROM job_postings jp
        LEFT JOIN companies c ON c.company_id = jp.company_id
        {skills_join}
        WHERE {where}
        ORDER BY {order_clause}
        LIMIT :page_size OFFSET :offset
        """
    )
    rows = db.execute(query, params).mappings().fetchall()

    jobs = []
    for row in rows:
        job_skills_rows = db.execute(
            text("SELECT skill_name FROM job_skills WHERE job_id = :jid ORDER BY skill_name"),
            {"jid": row["job_id"]},
        ).fetchall()
        skill_names = [r[0] for r in job_skills_rows]
        jobs.append(_build_job(dict(row), skill_names))

    next_cursor = None
    if len(jobs) == page_size and jobs:
        last = jobs[-1]
        next_cursor = _encode_cursor(str(last.job_id), last.posted_at)

    return APIResponse(
        data=jobs,
        meta=Meta(total_count=total, page_size=page_size, next_cursor=next_cursor),
    )


@router.get("/{job_id}", response_model=APIResponse[JobOut])
def get_job(job_id: UUID, db: Connection = Depends(get_db)):
    row = db.execute(
        text(
            """
            SELECT jp.*,
                   c.company_name, c.domain, c.industry, c.company_stage,
                   c.employee_count_range, c.hq_country
            FROM job_postings jp
            LEFT JOIN companies c ON c.company_id = jp.company_id
            WHERE jp.job_id = :job_id
            """
        ),
        {"job_id": str(job_id)},
    ).mappings().fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    skill_rows = db.execute(
        text("SELECT skill_name FROM job_skills WHERE job_id = :jid"),
        {"jid": str(job_id)},
    ).fetchall()
    skills = [r[0] for r in skill_rows]

    return APIResponse(data=_build_job(dict(row), skills))
