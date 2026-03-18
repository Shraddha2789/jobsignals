"""
Pydantic models for raw ingested job data.
These are the shapes that all source adapters must produce.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, field_validator


SourcePlatform = Literal[
    "linkedin",
    "indeed",
    "remoteok",
    "remotive",
    "arbeitnow",
    "adzuna",
    "jooble",
    "yc_jobs",
    "career_page",
    "seed",
    "other",
]
WorkModality = Literal["remote", "hybrid", "onsite", "unspecified"]
EmploymentType = Literal["full_time", "part_time", "contract", "internship"]
SeniorityLevel = Literal["intern", "junior", "mid", "senior", "staff", "principal", "executive"]


class RawCompany(BaseModel):
    name: str
    domain: Optional[str] = None
    industry: Optional[str] = None
    company_stage: Optional[str] = None
    employee_count_range: Optional[str] = None
    hq_country: str = "US"


class RawJobPosting(BaseModel):
    source_id: str
    source_platform: SourcePlatform
    source_url: Optional[str] = None

    title_raw: str
    company_name: str
    company_domain: Optional[str] = None

    location_raw: Optional[str] = None
    location_city: Optional[str] = None
    location_country: str = "US"
    work_modality: WorkModality = "unspecified"

    employment_type: EmploymentType = "full_time"
    seniority_level: Optional[SeniorityLevel] = None

    description_raw: str
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_currency: str = "USD"
    salary_source: Optional[Literal["posted", "inferred", "glassdoor_enrichment"]] = None

    posted_at: Optional[datetime] = None

    @field_validator("salary_min", "salary_max", mode="before")
    @classmethod
    def clamp_salary(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and (v < 10_000 or v > 2_000_000):
            return None
        return v
