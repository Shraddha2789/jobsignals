"""
Title normalization: maps raw job titles to canonical titles + role families.
Rule-based for MVP. Designed to be swapped for an LLM-based approach in Phase 2.
"""
from __future__ import annotations

import re

# ── Title family classification rules ────────────────────────────────────────
# Checked in order; first match wins.
# ML Engineering is checked before Software Engineering so "ai engineer" routes correctly.
FAMILY_RULES: list[tuple[list[str], str]] = [
    # ML Engineering
    (["ml engineer", "machine learning engineer", "mlops", "ml infra",
      "ai engineer", "llm engineer", "ai/ml engineer", "ml platform",
      "applied ml", "research engineer"],                                       "ML Engineering"),

    # Data Engineering
    (["data engineer", "analytics engineer", "etl engineer", "data infra",
      "data architect", "data platform", "data pipeline",
      "data infrastructure"],                                                   "Data Engineering"),

    # Data Science
    (["data scientist", "applied scientist", "research scientist",
      "data analyst", "planning & analytics",
      "quantitative analyst", "decision scientist"],                            "Data Science"),

    # Product Management
    (["product manager", " pm ", "apm", "group pm",
      "director of product", "director product",
      "head of product", "vp of product", "vp product", "chief product",
      "product lead", "product operations", "product owner"],                  "Product Management"),

    # Design
    (["ux designer", "ui designer", "ux/ui", "ui/ux", "product designer",
      "visual designer", "graphic designer", "web designer",
      "interaction designer", "design lead", "design manager",
      "brand designer", "motion designer"],                                    "Design"),

    # Marketing
    (["marketing manager", "marketing director", "growth manager",
      "content manager", "content strategist", "seo manager", "sem manager",
      "demand generation", "performance marketer", "social media manager",
      "email marketing", "brand manager", "communications manager",
      "marketing analyst", "growth hacker", "head of marketing",
      "vp marketing", "cmo"],                                                  "Marketing"),

    # Sales
    (["sales manager", "sales director", "account executive", "account manager",
      "sales engineer", "solutions engineer", "business development",
      "sales representative", "sales associate", "vp sales", "head of sales",
      "revenue manager", "partnerships manager", "client partner",
      "customer success manager", "csm"],                                      "Sales"),

    # Operations
    (["operations manager", "operations director", "chief of staff",
      "program manager", "project manager", "scrum master",
      "business analyst", "process analyst", "it manager",
      "it project manager", "supply chain", "logistics manager",
      "procurement manager", "revenue operations", "revops",
      "head of operations", "vp operations", "coo"],                          "Operations"),

    # Finance
    (["finance manager", "financial analyst", "fp&a", "controller",
      "accountant", "accounting manager", "cfo", "treasurer",
      "investment analyst", "financial planning", "tax manager",
      "audit manager", "actuary", "pricing analyst"],                          "Finance"),

    # HR
    (["hr manager", "hr director", "people operations", "recruiter",
      "talent acquisition", "recruiting coordinator", "hr business partner",
      "hrbp", "compensation manager", "benefits manager",
      "learning and development", "l&d", "head of people",
      "chief people officer", "vp people"],                                    "HR"),

    # Software Engineering — broadest, checked last among tech families
    (["software engineer", "software developer",
      "backend engineer", "backend developer", "back-end engineer",
      "frontend engineer", "frontend developer", "front-end engineer",
      "full stack", "fullstack", "full-stack",
      "platform engineer", "site reliability", "sre",
      "devops", "devsecops",
      "cloud engineer", "cloud systems engineer", "cloud developer",
      "cloud security",
      "security engineer", "security architect", "appsec",
      "firmware engineer", "embedded engineer",
      "mobile engineer", "mobile developer",
      "ios engineer", "ios developer",
      "android engineer", "android developer", "native android",
      "web developer",
      "tech lead", "technical lead",
      "engineering manager",
      "developer experience", "devex",
      "infrastructure engineer", "infra engineer",
      "automation engineer",
      "forward deployed engineer", "forward deployment engineer",
      "swe"],                                                                   "Software Engineering"),
]

# ── Seniority inference from title ───────────────────────────────────────────
SENIORITY_PATTERNS: list[tuple[str, str]] = [
    (r"\bintern\b",                         "intern"),
    (r"\bjunior\b|\bassociate\b|\bi\b",     "junior"),
    (r"\bstaff\b|\blead\b",                 "staff"),
    (r"\bprincipal\b|\bdistinguished\b|\bdirector\b", "principal"),
    (r"\bsenior\b|\bsr\.?\b|\biii\b|\biv\b","senior"),
    (r"\bii\b|\bmid\b",                     "mid"),
]

# ── Title cleaning ────────────────────────────────────────────────────────────
_NOISE = re.compile(
    r"\b(remote|hybrid|onsite|contract|part.time|full.time|usa|us|ny|sf|nyc)\b",
    re.I,
)


def normalize_title(raw: str) -> tuple[str, str | None, str | None]:
    """
    Returns (cleaned_title, title_family, seniority_level).
    All values are strings or None if not determinable.
    """
    cleaned = _NOISE.sub("", raw).strip(" ,/-")
    lower   = cleaned.lower()

    family = _classify_family(lower)
    seniority = _infer_seniority(lower)

    return cleaned, family, seniority


def _classify_family(lower: str) -> str | None:
    for keywords, family in FAMILY_RULES:
        if any(kw in lower for kw in keywords):
            return family
    return "Other"


def _infer_seniority(lower: str) -> str | None:
    for pattern, level in SENIORITY_PATTERNS:
        if re.search(pattern, lower):
            return level
    return "mid"  # default assumption
