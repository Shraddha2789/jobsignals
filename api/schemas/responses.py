"""API response schemas (Pydantic v2)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field

T = TypeVar("T")


# ── Envelope ──────────────────────────────────────────────────────────────────

class Meta(BaseModel):
    total_count: Optional[int] = None
    page_size:   Optional[int] = None
    next_cursor: Optional[str] = None
    window:      Optional[str] = None
    period_start: Optional[str] = None
    period_end:   Optional[str] = None

class APIResponse(BaseModel, Generic[T]):
    data:   T
    meta:   Optional[Meta]   = None
    errors: Optional[list[str]] = None


# ── Company ───────────────────────────────────────────────────────────────────

class CompanyOut(BaseModel):
    company_id:           UUID
    company_name:         str
    domain:               Optional[str]
    industry:             Optional[str]
    company_stage:        Optional[str]
    employee_count_range: Optional[str]
    hq_country:           Optional[str]

    model_config = {"from_attributes": True}


# ── Job Posting ───────────────────────────────────────────────────────────────

class SalaryOut(BaseModel):
    min:      Optional[int]
    max:      Optional[int]
    currency: str
    source:   Optional[str]


class LocationOut(BaseModel):
    raw:      Optional[str]
    city:     Optional[str]
    country:  Optional[str]
    modality: str


class JobOut(BaseModel):
    job_id:           UUID
    source_platform:  str
    source_url:       Optional[str]
    title_raw:        str
    title_normalized: Optional[str]
    title_family:     Optional[str]
    seniority_level:  Optional[str]
    company:          Optional[CompanyOut]
    location:         LocationOut
    employment_type:  str
    salary:           Optional[SalaryOut]
    skills:           list[str] = Field(default_factory=list)
    posted_at:        Optional[datetime]
    is_active:        bool

    model_config = {"from_attributes": True}


# ── Skill Trend ───────────────────────────────────────────────────────────────

class SkillTrendOut(BaseModel):
    skill_name:      str
    skill_category:  Optional[str] = None
    posting_count:   int
    posting_share:   float
    mom_change:      Optional[float]
    yoy_change:      Optional[float]
    rank:            int


# ── Company Signals ───────────────────────────────────────────────────────────

class HiringTrendPoint(BaseModel):
    period:   str
    postings: int


class CompanySignalsOut(BaseModel):
    company_id:             UUID
    company_name:           str
    window:                 str
    total_postings:         int
    active_postings:        int
    hiring_velocity_score:  Optional[float]
    top_skills:             list[str]
    top_roles:              list[str]
    salary_benchmarks:      Optional[dict[str, Any]]
    trend:                  list[HiringTrendPoint] = Field(default_factory=list)


# ── Salary Benchmark ──────────────────────────────────────────────────────────

class SalaryBenchmarkOut(BaseModel):
    title_family:   str
    seniority:      str
    country:        str
    percentile_10:  Optional[int]
    percentile_25:  Optional[int]
    percentile_50:  Optional[int]
    percentile_75:  Optional[int]
    percentile_90:  Optional[int]
    currency:       str = "USD"
    sample_size:    int
    period:         str


# ── AI Insights ───────────────────────────────────────────────────────────────

class InsightRequest(BaseModel):
    question:     str            = Field(..., min_length=5, max_length=500, description="Natural language question about the job market")
    title_family: Optional[str]  = Field(None, description="Scope analysis to a role family")
    country:      Optional[str]  = Field("US", description="ISO 2-letter country code")
    window:       Optional[int]  = Field(90,   description="Look-back window in days", ge=7, le=365)


class InsightOut(BaseModel):
    question:   str
    analysis:   str
    sources:    list[str]        = Field(default_factory=list, description="Data endpoints used to answer the question")
    model:      str
