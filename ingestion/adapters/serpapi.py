"""
SerpAPI adapter — fetches job postings via Google Jobs search.

Free tier: 100 searches/month (https://serpapi.com/pricing)
Sign up: https://serpapi.com/users/sign_up — get your API key from the dashboard.
Set env var: SERPAPI_KEY=your_key

Searches rotate through India cities + global tech hubs each run.
Designed to run ONCE PER DAY to stay within free tier limits.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
from datetime import datetime, timezone
from typing import Iterator

import httpx
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

console = Console()

API_URL = "https://serpapi.com/search.json"

# ── Search targets — rotated each run ─────────────────────────────────────────
# India cities + global tech hubs. Each run picks a slice to stay within limits.
SEARCH_TARGETS = [
    # India
    {"q": "software engineer", "location": "Bangalore, India", "country": "IN"},
    {"q": "product manager", "location": "Mumbai, India", "country": "IN"},
    {"q": "data scientist", "location": "Hyderabad, India", "country": "IN"},
    {"q": "data engineer", "location": "Pune, India", "country": "IN"},
    {"q": "backend developer", "location": "Delhi, India", "country": "IN"},
    {"q": "frontend developer", "location": "Chennai, India", "country": "IN"},
    {"q": "machine learning", "location": "Bangalore, India", "country": "IN"},
    {"q": "devops engineer", "location": "Hyderabad, India", "country": "IN"},
    # Global
    {"q": "software engineer", "location": "London, UK", "country": "GB"},
    {"q": "product manager", "location": "New York, USA", "country": "US"},
    {"q": "data scientist", "location": "Berlin, Germany", "country": "DE"},
    {"q": "software engineer", "location": "Toronto, Canada", "country": "CA"},
    {"q": "data engineer", "location": "Sydney, Australia", "country": "AU"},
    {"q": "product manager", "location": "Singapore", "country": "SG"},
]

# Run 2 searches per day → 60/month → well within 100 free limit
SEARCHES_PER_RUN = 2


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _today_index() -> int:
    """Return a number that changes daily — used to rotate through targets."""
    return datetime.now(tz=timezone.utc).timetuple().tm_yday  # 1–365


class SerpAPIAdapter(BaseAdapter):
    """
    Fetches jobs from Google Jobs via SerpAPI.
    Rotates search queries daily across India cities and global hubs.

    Set SERPAPI_KEY env var. Free tier: 100 searches/month.
    Sign up: https://serpapi.com/users/sign_up
    """

    source_platform = "other"

    def __init__(self, searches_per_run: int = SEARCHES_PER_RUN) -> None:
        self.api_key = os.environ.get("SERPAPI_KEY", "")
        self.searches_per_run = searches_per_run

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=15),
    )
    def _search(self, q: str, location: str) -> list[dict]:
        params = {
            "engine": "google_jobs",
            "q": q,
            "location": location,
            "api_key": self.api_key,
            "num": 10,
        }
        resp = httpx.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json().get("jobs_results", [])

    def fetch(self) -> Iterator[RawJobPosting]:
        if not self.api_key:
            console.print("  [yellow]⚠ SERPAPI_KEY not set — skipping Google Jobs[/]")
            return

        console.print("  [dim]Fetching from Google Jobs via SerpAPI...[/]")

        # Pick today's slice of search targets (rotates daily)
        start = (_today_index() * self.searches_per_run) % len(SEARCH_TARGETS)
        targets = SEARCH_TARGETS[start : start + self.searches_per_run]

        total = 0
        for target in targets:
            q = target["q"]
            location = target["location"]
            country = target["country"]

            try:
                jobs = self._search(q, location)
            except Exception as e:
                console.print(f"  [red]✗ SerpAPI '{q}' in '{location}' failed: {e}[/]")
                continue

            console.print(f"  [dim]  Google Jobs '{q}' @ {location}: {len(jobs)} results[/]")

            for job in jobs:
                description = _strip_html(job.get("description") or job.get("snippet") or "")
                if not description:
                    continue

                # Build a stable source_id from title + company + location
                raw_id = f"{job.get('title','')}{job.get('company_name','')}{location}"
                source_id = "serp_" + hashlib.md5(raw_id.encode()).hexdigest()[:12]

                apply_link = ""
                options = job.get("apply_options") or []
                if options:
                    apply_link = options[0].get("link", "")

                # Detect work modality
                detected_loc = (job.get("location") or "").lower()
                if "remote" in detected_loc:
                    modality = "remote"
                elif "hybrid" in detected_loc:
                    modality = "hybrid"
                else:
                    modality = "onsite"

                yield RawJobPosting(
                    source_id=source_id,
                    source_platform="other",
                    source_url=apply_link or None,
                    title_raw=job.get("title") or q,
                    company_name=job.get("company_name") or "Unknown",
                    company_domain=None,
                    location_raw=job.get("location") or location,
                    location_city=location.split(",")[0].strip(),
                    location_country=country,
                    work_modality=modality,
                    employment_type="full_time",
                    seniority_level=None,
                    description_raw=description,
                    salary_min=None,
                    salary_max=None,
                    salary_currency="INR" if country == "IN" else "USD",
                    salary_source=None,
                    posted_at=_parse_date(job.get("detected_extensions", {}).get("posted_at")),
                )
                total += 1

            time.sleep(2)  # be polite between searches

        console.print(f"  [green]✓[/] SerpAPI (Google Jobs): {total} jobs fetched")
