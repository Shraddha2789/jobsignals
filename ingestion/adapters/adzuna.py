"""
Adzuna adapter — fetches job postings from Adzuna's public API.

Adzuna is the richest free source: salary data, location, 50+ countries.
Free tier: 100 calls/day × 50 results = 5,000 jobs/day.

Register at: https://developer.adzuna.com/
Set in .env:
    ADZUNA_APP_ID=...
    ADZUNA_APP_KEY=...

Run: python -m scripts.ingest_real
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Iterator

import httpx
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

console = Console()

BASE_URL = "https://api.adzuna.com/v1/api/jobs"
RESULTS_PER_PAGE = 50

# Search terms to get diverse results across families
SEARCH_QUERIES = [
    "software engineer",
    "data engineer",
    "data scientist",
    "product manager",
    "machine learning",
    "marketing manager",
    "finance analyst",
    "operations manager",
    "sales manager",
    "hr manager",
    "ux designer",
    "devops engineer",
]

COUNTRY = "us"   # Adzuna country code — us, gb, ca, au, de, fr, etc.


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    for entity, char in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                          ("&nbsp;", " "), ("&#39;", "'"), ("&quot;", '"')]:
        text = text.replace(entity, char)
    return re.sub(r"\s+", " ", text).strip()


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _infer_modality(title: str, desc: str) -> str:
    combined = (title + " " + desc).lower()
    if "remote" in combined:
        return "remote"
    if "hybrid" in combined:
        return "hybrid"
    return "onsite"


class AdzunaAdapter(BaseAdapter):
    """
    Fetches job postings from Adzuna across multiple search queries.
    Each query returns up to `pages` × 50 results. Deduplicates by Adzuna job ID.
    Includes salary_min/salary_max when Adzuna provides them.
    """

    source_platform = "adzuna"

    def __init__(
        self,
        app_id: str | None = None,
        app_key: str | None = None,
        country: str = COUNTRY,
        queries: list[str] | None = None,
        pages_per_query: int = 1,
    ) -> None:
        self.app_id  = app_id  or os.environ.get("ADZUNA_APP_ID", "")
        self.app_key = app_key or os.environ.get("ADZUNA_APP_KEY", "")
        self.country = country
        self.queries = queries or SEARCH_QUERIES
        self.pages_per_query = pages_per_query

    def _is_configured(self) -> bool:
        return bool(self.app_id and self.app_key)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_page(self, query: str, page: int) -> list[dict]:
        url = f"{BASE_URL}/{self.country}/search/{page}"
        response = httpx.get(
            url,
            params={
                "app_id":          self.app_id,
                "app_key":         self.app_key,
                "results_per_page": RESULTS_PER_PAGE,
                "what":            query,
                "content-type":    "application/json",
            },
            timeout=20,
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def fetch(self) -> Iterator[RawJobPosting]:
        if not self._is_configured():
            console.print("  [yellow]⚠ Adzuna skipped — ADZUNA_APP_ID / ADZUNA_APP_KEY not set in .env[/]")
            return

        console.print(f"  [dim]Fetching from Adzuna ({len(self.queries)} queries × {self.pages_per_query} page)...[/]")
        seen_ids: set[str] = set()
        total_raw = 0
        yielded   = 0

        for query in self.queries:
            for page in range(1, self.pages_per_query + 1):
                try:
                    jobs = self._fetch_page(query, page)
                except Exception as e:
                    console.print(f"  [red]✗ Adzuna '{query}' page {page} failed: {e}[/]")
                    break

                total_raw += len(jobs)
                if not jobs:
                    break

                for job in jobs:
                    job_id = str(job.get("id", ""))
                    if not job_id or job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    title = (job.get("title") or "").strip()
                    if not title:
                        continue

                    desc_raw = _strip_html(job.get("description") or "")
                    if not desc_raw:
                        continue

                    # Location
                    loc_obj  = job.get("location") or {}
                    loc_area = loc_obj.get("area") or []
                    city     = loc_area[-1] if loc_area else None
                    country_display = loc_area[0] if loc_area else self.country.upper()
                    loc_raw  = loc_obj.get("display_name") or city or "Unknown"
                    country_code = self.country.upper()

                    # Salary — Adzuna provides salary_min/salary_max when known
                    sal_min = job.get("salary_min")
                    sal_max = job.get("salary_max")
                    sal_src = "posted" if sal_min else None
                    if sal_min:
                        sal_min = int(sal_min)
                    if sal_max:
                        sal_max = int(sal_max)

                    # Company
                    company_obj  = job.get("company") or {}
                    company_name = (company_obj.get("display_name") or "Unknown").strip()

                    modality = _infer_modality(title, desc_raw)

                    currency_map = {
                        "us": "USD", "gb": "GBP", "ca": "CAD", "au": "AUD",
                        "de": "EUR", "fr": "EUR", "nl": "EUR", "in": "INR",
                        "sg": "SGD", "nz": "NZD",
                    }
                    yield RawJobPosting(
                        source_id        = job_id,
                        source_platform  = "adzuna",
                        source_url       = job.get("redirect_url"),
                        title_raw        = title,
                        company_name     = company_name,
                        company_domain   = None,
                        location_raw     = loc_raw,
                        location_city    = city,
                        location_country = country_code,
                        work_modality    = modality,
                        employment_type  = "full_time",
                        seniority_level  = None,
                        description_raw  = desc_raw,
                        salary_min       = sal_min,
                        salary_max       = sal_max,
                        salary_currency  = currency_map.get(self.country, "USD"),
                        salary_source    = sal_src,
                        posted_at        = _parse_date(job.get("created")),
                    )
                    yielded += 1

        console.print(f"  [dim]Received {total_raw} raw jobs from Adzuna ({len(seen_ids)} unique)[/]")
        console.print(f"  [green]✓[/] {yielded} Adzuna jobs yielded")
