"""
Himalayas adapter — fetches remote job postings from himalayas.app public API.

Completely free, no API key required.
Focuses on remote jobs from India-based companies and global remote roles.

Docs: https://himalayas.app/api
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

API_URL = "https://himalayas.app/jobs/api"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) JobSignals/1.0",
    "Accept": "application/json",
}
PAGE_SIZE = 20  # Himalayas max per request
MAX_PAGES = 5  # 100 jobs per run — stays well within free limits


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html or "")
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&#\d+;", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _infer_country(company: dict) -> str:
    """Use company HQ country if available."""
    country = (company.get("headquarters") or "").strip()
    if not country:
        return "US"
    mapping = {
        "india": "IN",
        "united states": "US",
        "us": "US",
        "usa": "US",
        "united kingdom": "GB",
        "uk": "GB",
        "germany": "DE",
        "canada": "CA",
        "australia": "AU",
        "singapore": "SG",
    }
    return mapping.get(country.lower(), "US")


class HimalayasAdapter(BaseAdapter):
    """
    Fetches remote jobs from himalayas.app — no auth required.
    Particularly useful for India-based remote roles.
    """

    source_platform = "other"  # stored as "himalayas" via source_id prefix

    def __init__(self, max_pages: int = MAX_PAGES) -> None:
        self.max_pages = max_pages

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _fetch_page(self, offset: int) -> dict:
        params = {"limit": PAGE_SIZE, "offset": offset}
        resp = httpx.get(API_URL, headers=HEADERS, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def fetch(self) -> Iterator[RawJobPosting]:
        console.print("  [dim]Fetching from Himalayas API...[/]")
        total_yielded = 0

        for page in range(self.max_pages):
            offset = page * PAGE_SIZE
            try:
                data = self._fetch_page(offset)
            except Exception as e:
                console.print(f"  [red]✗ Himalayas page {page+1} failed: {e}[/]")
                break

            jobs = data.get("jobs", [])
            if not jobs:
                break

            for job in jobs:
                description = _strip_html(job.get("description") or "")
                if not description:
                    continue

                company = job.get("company") or {}
                title = job.get("title") or ""
                source_id = str(job.get("id") or job.get("slug") or f"him_{title[:20]}")

                yield RawJobPosting(
                    source_id=f"himalayas_{source_id}",
                    source_platform="other",
                    source_url=f"https://himalayas.app/jobs/{job.get('slug', source_id)}",
                    title_raw=title,
                    company_name=company.get("name") or "Unknown",
                    company_domain=None,
                    location_raw="Remote",
                    location_city=company.get("headquarters") or "Remote",
                    location_country=_infer_country(company),
                    work_modality="remote",
                    employment_type="full_time",
                    seniority_level=None,
                    description_raw=description,
                    salary_min=None,
                    salary_max=None,
                    salary_currency="USD",
                    salary_source=None,
                    posted_at=_parse_date(job.get("createdAt") or job.get("publishedAt")),
                )
                total_yielded += 1

            # Respect rate limits
            time.sleep(1)

        console.print(f"  [green]✓[/] Himalayas: {total_yielded} jobs fetched")
