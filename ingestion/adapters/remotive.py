"""
Remotive adapter — fetches remote job postings from remotive.com's public API.

Fetches per-category across all known categories to overcome the 22-job free-tier
cap on the undifferentiated endpoint. Each category call returns up to ~150 jobs.
No auth required. HTML descriptions included.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterator

import httpx
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

console = Console()

API_URL = "https://remotive.com/api/remote-jobs"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) JobSignals/1.0",
    "Accept": "application/json",
}

# All known Remotive category slugs
CATEGORIES = [
    "software-development",
    "customer-service",
    "design",
    "marketing",
    "sales-business",
    "product",
    "ai-ml",
    "data",
    "devops",
    "finance",
    "human-resources",
    "qa",
    "writing",
    "all-others",
]

# Remotive job_type → our employment_type enum
JOB_TYPE_MAP = {
    "full_time":  "full_time",
    "part_time":  "part_time",
    "contract":   "contract",
    "internship": "internship",
    "freelance":  "contract",
}


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    for entity, char in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                          ("&nbsp;", " "), ("&#39;", "'"), ("&quot;", '"')]:
        text = text.replace(entity, char)
    return re.sub(r"\s+", " ", text).strip()


def _parse_salary(salary_str: str | None) -> tuple[int | None, int | None]:
    """Parse Remotive's free-text salary like '$80k-$120k' or '$100,000'."""
    if not salary_str:
        return None, None
    s = salary_str.lower().replace(",", "").replace(" ", "")
    numbers = []
    for m in re.finditer(r"\$?(\d+(?:\.\d+)?)(k?)", s):
        val = float(m.group(1))
        if m.group(2) == "k":
            val *= 1000
        if val >= 10_000:
            numbers.append(int(val))
    if not numbers:
        return None, None
    if len(numbers) == 1:
        return numbers[0], None
    return min(numbers[0], numbers[1]), max(numbers[0], numbers[1])


def _parse_date(date_str: str) -> datetime | None:
    try:
        return datetime.fromisoformat(date_str).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


_COUNTRY_HINTS: dict[str, str] = {
    "india": "IN", "bangalore": "IN", "mumbai": "IN", "delhi": "IN", "hyderabad": "IN",
    "united kingdom": "GB", "uk": "GB", "london": "GB",
    "germany": "DE", "berlin": "DE", "munich": "DE",
    "canada": "CA", "toronto": "CA", "vancouver": "CA",
    "australia": "AU", "sydney": "AU", "melbourne": "AU",
    "singapore": "SG",
    "france": "FR", "paris": "FR",
    "netherlands": "NL", "amsterdam": "NL",
    "brazil": "BR", "são paulo": "BR",
}

def _infer_country_remotive(location: str) -> str:
    if not location:
        return "US"
    loc = location.lower()
    for hint, code in _COUNTRY_HINTS.items():
        if hint in loc:
            return code
    return "US"


class RemotiveAdapter(BaseAdapter):
    """
    Fetches live remote job postings from remotive.com.
    Iterates all categories to work around the per-request job cap.
    """

    source_platform = "remotive"

    def __init__(self, categories: list[str] | None = None) -> None:
        self.categories = categories or CATEGORIES

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_category(self, slug: str) -> list[dict]:
        response = httpx.get(
            API_URL, headers=HEADERS, params={"category": slug}, timeout=20
        )
        response.raise_for_status()
        return response.json().get("jobs", [])

    def fetch(self) -> Iterator[RawJobPosting]:
        console.print(f"  [dim]Fetching from Remotive API ({len(self.categories)} categories)...[/]")
        seen_ids: set[str] = set()
        total_raw = 0
        yielded = 0

        for slug in self.categories:
            try:
                raw_jobs = self._fetch_category(slug)
            except Exception as e:
                console.print(f"  [red]✗ Remotive category '{slug}' failed: {e}[/]")
                continue

            total_raw += len(raw_jobs)

            for job in raw_jobs:
                job_id_str = str(job["id"])
                if job_id_str in seen_ids:
                    continue
                seen_ids.add(job_id_str)

                description_raw = _strip_html(job.get("description") or "")
                if not description_raw:
                    continue

                sal_min, sal_max = _parse_salary(job.get("salary"))
                loc_raw = job.get("candidate_required_location") or "Remote"
                emp_type = JOB_TYPE_MAP.get(
                    (job.get("job_type") or "").lower(), "full_time"
                )

                yield RawJobPosting(
                    source_id        = job_id_str,
                    source_platform  = "remotive",
                    source_url       = job.get("url"),
                    title_raw        = job["title"],
                    company_name     = (job.get("company_name") or "Unknown").strip(),
                    company_domain   = None,
                    location_raw     = loc_raw,
                    location_city    = "Remote",
                    location_country = _infer_country_remotive(loc_raw),
                    work_modality    = "remote",
                    employment_type  = emp_type,
                    seniority_level  = None,
                    description_raw  = description_raw,
                    salary_min       = sal_min,
                    salary_max       = sal_max,
                    salary_currency  = "USD",
                    salary_source    = "posted" if sal_min else None,
                    posted_at        = _parse_date(job.get("publication_date", "")),
                )
                yielded += 1

        console.print(f"  [dim]Received {total_raw} raw jobs from Remotive ({len(seen_ids)} unique)[/]")
        console.print(f"  [green]✓[/] {yielded} Remotive jobs yielded")
