"""
Jooble adapter — fetches job postings from Jooble's free public API.

Jooble aggregates listings from 71+ countries including India, UK, US, Germany, etc.
Free tier: ~5,000 requests/month.

Register for a free key at: https://jooble.org/api/about
Set in .env:
    JOOBLE_API_KEY=your-key-here

Jooble returns up to 20 jobs per page. We paginate across multiple keywords.
"""
from __future__ import annotations

import json
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

BASE_URL = "https://jooble.org/api"

# Search queries × target countries for broad coverage
QUERY_MATRIX = [
    # (keyword, country_name_for_jooble, iso2)
    ("software engineer", "India", "IN"),
    ("data engineer", "India", "IN"),
    ("product manager", "India", "IN"),
    ("data scientist", "India", "IN"),
    ("machine learning", "India", "IN"),
    ("software engineer", "United Kingdom", "GB"),
    ("data engineer", "United Kingdom", "GB"),
    ("software engineer", "Canada", "CA"),
    ("data scientist", "Canada", "CA"),
    ("software engineer", "Australia", "AU"),
    ("data engineer", "Australia", "AU"),
    ("software engineer", "Singapore", "SG"),
]

COUNTRY_CURRENCY = {
    "IN": "INR", "GB": "GBP", "CA": "CAD", "AU": "AUD",
    "SG": "SGD", "US": "USD", "DE": "EUR",
}


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    for entity, char in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                          ("&nbsp;", " "), ("&#39;", "'"), ("&quot;", '"')]:
        text = text.replace(entity, char)
    return re.sub(r"\s+", " ", text).strip()


def _parse_salary(salary_str: str | None) -> tuple[int | None, int | None]:
    if not salary_str:
        return None, None
    nums = re.findall(r"[\d,]+", str(salary_str).replace(",", ""))
    vals = [int(n) for n in nums if int(n) > 1000]
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], None
    return min(vals[0], vals[1]), max(vals[0], vals[1])


def _infer_modality(title: str, desc: str) -> str:
    combined = (title + " " + desc).lower()
    if "remote" in combined:
        return "remote"
    if "hybrid" in combined:
        return "hybrid"
    return "onsite"


class JoobleAdapter(BaseAdapter):
    """
    Fetches job postings from Jooble across multiple countries.
    Primary use case: India and other markets not covered by RemoteOK/Remotive.
    """

    source_platform = "jooble"

    def __init__(
        self,
        api_key: str | None = None,
        query_matrix: list[tuple[str, str, str]] | None = None,
        pages_per_query: int = 2,
    ) -> None:
        self.api_key = api_key or os.environ.get("JOOBLE_API_KEY", "")
        self.query_matrix = query_matrix or QUERY_MATRIX
        self.pages_per_query = pages_per_query

    def _is_configured(self) -> bool:
        return bool(self.api_key)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_page(self, keyword: str, location: str, page: int) -> list[dict]:
        url = f"{BASE_URL}/{self.api_key}"
        payload = {"keywords": keyword, "location": location, "page": page}
        response = httpx.post(
            url,
            content=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        response.raise_for_status()
        return response.json().get("jobs", [])

    def fetch(self) -> Iterator[RawJobPosting]:
        if not self._is_configured():
            console.print("  [yellow]⚠ Jooble skipped — JOOBLE_API_KEY not set in .env[/]")
            return

        console.print(f"  [dim]Fetching from Jooble API ({len(self.query_matrix)} queries)...[/]")
        seen_ids: set[str] = set()
        total_raw = 0
        yielded = 0

        for keyword, country_name, iso2 in self.query_matrix:
            for page in range(1, self.pages_per_query + 1):
                try:
                    jobs = self._fetch_page(keyword, country_name, page)
                except Exception as e:
                    console.print(f"  [red]✗ Jooble '{keyword}/{country_name}' page {page} failed: {e}[/]")
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

                    desc_raw = _strip_html(job.get("snippet") or job.get("description") or "")
                    if not desc_raw:
                        continue

                    company = (job.get("company") or "Unknown").strip()
                    location_raw = (job.get("location") or country_name).strip()
                    city = location_raw.split(",")[0].strip() if location_raw else None

                    sal_min, sal_max = _parse_salary(job.get("salary"))

                    yield RawJobPosting(
                        source_id        = job_id,
                        source_platform  = "jooble",
                        source_url       = job.get("link"),
                        title_raw        = title,
                        company_name     = company,
                        company_domain   = None,
                        location_raw     = location_raw,
                        location_city    = city,
                        location_country = iso2,
                        work_modality    = _infer_modality(title, desc_raw),
                        employment_type  = "full_time",
                        seniority_level  = None,
                        description_raw  = desc_raw,
                        salary_min       = sal_min,
                        salary_max       = sal_max,
                        salary_currency  = COUNTRY_CURRENCY.get(iso2, "USD"),
                        salary_source    = "posted" if sal_min else None,
                        posted_at        = datetime.now(tz=timezone.utc),
                    )
                    yielded += 1

        console.print(f"  [dim]Received {total_raw} raw Jooble jobs ({len(seen_ids)} unique)[/]")
        console.print(f"  [green]✓[/] {yielded} Jooble jobs yielded")
