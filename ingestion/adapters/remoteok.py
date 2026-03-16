"""
RemoteOK adapter — fetches real job postings from remoteok.com's public JSON API.

RemoteOK is a fully public remote jobs board with no auth required.
It returns ~100 recent postings on each request with salary data, tags, and descriptions.

This is the first real data source for JobSignals.
Replace SeedAdapter with this (or run both) in pipeline/runner.py.
"""
from __future__ import annotations

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

API_URL = "https://remoteok.com/api"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) JobSignals/1.0",
    "Accept": "application/json",
}

# RemoteOK tags that map to our role families
TECH_TAGS = {
    "data engineer", "data engineering", "data science", "machine learning",
    "ml", "ai", "backend", "frontend", "devops", "python", "software engineer",
    "product manager", "pm", "analyst", "analytics"
}


_LOCATION_COUNTRY_MAP = {
    "germany": "DE", "berlin": "DE", "munich": "DE", "hamburg": "DE",
    "cologne": "DE", "frankfurt": "DE", "düsseldorf": "DE", "stuttgart": "DE",
    "united kingdom": "GB", "uk": "GB", "london": "GB", "manchester": "GB",
    "edinburgh": "GB", "birmingham": "GB",
    "canada": "CA", "toronto": "CA", "vancouver": "CA", "montreal": "CA",
    "australia": "AU", "sydney": "AU", "melbourne": "AU", "brisbane": "AU",
    "india": "IN", "bangalore": "IN", "bengaluru": "IN", "mumbai": "IN",
    "delhi": "IN", "hyderabad": "IN", "pune": "IN",
    "netherlands": "NL", "amsterdam": "NL",
    "france": "FR", "paris": "FR",
    "singapore": "SG",
    "philippines": "PH", "manila": "PH", "metro manila": "PH",
    "freiburg": "DE", "aschersleben": "DE", "augsburg": "DE",
    "karlsruhe": "DE", "potsdam": "DE", "dresden": "DE",
    "hannover": "DE", "hanover": "DE", "nuremberg": "DE", "nürnberg": "DE",
    "dortmund": "DE", "essen": "DE", "bremen": "DE", "leipzig": "DE",
    "bonn": "DE", "am main": "DE", "im breisgau": "DE",
    "brazil": "BR", "são paulo": "BR", "sao paulo": "BR",
    "united states": "US", "usa": "US",
}

def _infer_country_from_location(location: str) -> str:
    if not location:
        return "US"
    loc = location.lower()
    for hint, code in _LOCATION_COUNTRY_MAP.items():
        if hint in loc:
            return code
    return "US"


def _strip_html(html: str) -> str:
    """Remove HTML tags and clean up whitespace."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&amp;",  "&",  text)
    text = re.sub(r"&lt;",   "<",  text)
    text = re.sub(r"&gt;",   ">",  text)
    text = re.sub(r"&nbsp;", " ",  text)
    text = re.sub(r"&#\d+;", " ",  text)
    text = re.sub(r"\s+",    " ",  text)
    return text.strip()


def _parse_salary(raw_min, raw_max) -> tuple[int | None, int | None]:
    """
    Normalise salary values. RemoteOK returns integers or 0 for unknown.
    """
    lo = int(raw_min) if raw_min and int(raw_min) > 10_000 else None
    hi = int(raw_max) if raw_max and int(raw_max) > 10_000 else None
    # Sanity: max must be >= min
    if lo and hi and hi < lo:
        lo, hi = hi, lo
    return lo, hi


def _parse_date(date_str: str) -> datetime | None:
    """Parse ISO 8601 date string from RemoteOK."""
    try:
        return datetime.fromisoformat(date_str).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


class RemoteOKAdapter(BaseAdapter):
    """
    Fetches live remote job postings from remoteok.com.

    Usage:
        adapter = RemoteOKAdapter()
        for posting in adapter.fetch():
            ...
    """

    source_platform = "remoteok"

    def __init__(self, min_relevance_tags: int = 0) -> None:
        """
        min_relevance_tags: only include jobs that have at least this many
        tech-relevant tags. Set to 0 to include all jobs.
        """
        self.min_relevance_tags = min_relevance_tags

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _fetch_raw(self) -> list[dict]:
        """Fetch raw JSON from RemoteOK API with retry logic."""
        response = httpx.get(API_URL, headers=HEADERS, timeout=20)
        response.raise_for_status()
        data = response.json()
        # First element is a legal notice dict — skip it
        return [item for item in data if item.get("id") and item.get("position")]

    def fetch(self) -> Iterator[RawJobPosting]:
        console.print(f"  [dim]Fetching from RemoteOK API...[/]")

        try:
            raw_jobs = self._fetch_raw()
        except Exception as e:
            console.print(f"  [red]✗ RemoteOK fetch failed: {e}[/]")
            return

        console.print(f"  [dim]Received {len(raw_jobs)} jobs from RemoteOK[/]")
        yielded = 0

        for job in raw_jobs:
            # Optional: filter by tech relevance
            tags = [t.lower() for t in (job.get("tags") or [])]
            if self.min_relevance_tags > 0:
                relevant = sum(1 for t in tags if any(kw in t for kw in TECH_TAGS))
                if relevant < self.min_relevance_tags:
                    continue

            description_raw = _strip_html(job.get("description") or "")
            if not description_raw:
                continue

            sal_min, sal_max = _parse_salary(
                job.get("salary_min"), job.get("salary_max")
            )

            # Location — infer country from the location field when present
            location_raw = job.get("location") or "Remote"
            city = location_raw
            country = _infer_country_from_location(location_raw)

            yield RawJobPosting(
                source_id       = str(job["id"]),
                source_platform = "remoteok",
                source_url      = job.get("url") or job.get("apply_url"),
                title_raw       = job["position"],
                company_name    = job.get("company") or "Unknown",
                company_domain  = None,
                location_raw    = location_raw,
                location_city   = city,
                location_country= country,
                work_modality   = "remote",
                employment_type = "full_time",
                seniority_level = None,         # will be inferred by normalizer
                description_raw = description_raw,
                salary_min      = sal_min,
                salary_max      = sal_max,
                salary_currency = "USD",
                salary_source   = "posted" if sal_min else None,
                posted_at       = _parse_date(job.get("date", "")),
            )
            yielded += 1

        console.print(f"  [green]✓[/] {yielded} jobs passed filters")
