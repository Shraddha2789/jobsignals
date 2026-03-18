"""
We Work Remotely adapter — fetches remote job postings via public RSS feed.

Completely free, no API key required.
One of the largest remote job boards. Strong US/global remote coverage.

RSS feed: https://weworkremotely.com/remote-jobs.rss
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Iterator
from xml.etree import ElementTree as ET

import httpx
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

console = Console()

RSS_URL = "https://weworkremotely.com/remote-jobs.rss"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) JobSignals/1.0",
    "Accept": "application/rss+xml, application/xml, text/xml",
}

# Namespaces used in the RSS feed
NS = {"wwr": "https://weworkremotely.com"}


def _strip_html(html: str) -> str:
    text = re.sub(r"<!\[CDATA\[", "", html or "")
    text = re.sub(r"\]\]>", "", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(tz=timezone.utc)
    try:
        # RFC 2822 format: "Mon, 18 Mar 2026 12:00:00 +0000"
        from email.utils import parsedate_to_datetime

        return parsedate_to_datetime(date_str).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _infer_country(region: str) -> str:
    region_lower = region.lower()
    mapping = {
        "usa": "US",
        "us only": "US",
        "united states": "US",
        "uk": "GB",
        "united kingdom": "GB",
        "canada": "CA",
        "australia": "AU",
        "germany": "DE",
        "india": "IN",
        "europe": "GB",  # default Europe to GB
    }
    for key, code in mapping.items():
        if key in region_lower:
            return code
    return "US"  # WWR is mostly US-centric


class WeWorkRemotelyAdapter(BaseAdapter):
    """
    Fetches remote jobs from We Work Remotely via public RSS.
    No auth required. Top remote job board globally.
    """

    source_platform = "other"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _fetch_rss(self) -> bytes:
        resp = httpx.get(RSS_URL, headers=HEADERS, timeout=30, follow_redirects=True)
        resp.raise_for_status()
        return resp.content

    def fetch(self) -> Iterator[RawJobPosting]:
        console.print("  [dim]Fetching from We Work Remotely RSS...[/]")
        total = 0

        try:
            raw_xml = self._fetch_rss()
            root = ET.fromstring(raw_xml)
        except Exception as e:
            console.print(f"  [red]✗ We Work Remotely fetch failed: {e}[/]")
            return

        items = root.findall(".//item")
        console.print(f"  [dim]  Got {len(items)} items from WWR feed[/]")

        for item in items:
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            pubdate_el = item.find("pubDate")
            guid_el = item.find("guid")
            region_el = item.find("{https://weworkremotely.com}region")
            company_el = item.find("{https://weworkremotely.com}company")

            raw_title = _strip_html(title_el.text or "") if title_el is not None else ""
            if not raw_title:
                continue

            description = _strip_html(desc_el.text or "") if desc_el is not None else ""
            if not description:
                description = raw_title  # fallback

            guid = (guid_el.text or "").strip() if guid_el is not None else ""
            source_id = "wwr_" + hashlib.md5(guid.encode()).hexdigest()[:12]
            link = (link_el.text or "").strip() if link_el is not None else None
            region = _strip_html(region_el.text or "") if region_el is not None else "Worldwide"
            company = _strip_html(company_el.text or "") if company_el is not None else "Unknown"

            # Title often has format "Company: Job Title at Company"
            # Clean it up
            job_title = raw_title
            if ": " in raw_title:
                parts = raw_title.split(": ", 1)
                if len(parts) == 2:
                    company = company or parts[0].strip()
                    job_title = parts[1].strip()

            country = _infer_country(region)

            yield RawJobPosting(
                source_id=source_id,
                source_platform="other",
                source_url=link,
                title_raw=job_title,
                company_name=company or "Unknown",
                company_domain=None,
                location_raw=region or "Worldwide",
                location_city=region or "Remote",
                location_country=country,
                work_modality="remote",
                employment_type="full_time",
                seniority_level=None,
                description_raw=description,
                salary_min=None,
                salary_max=None,
                salary_currency="USD",
                salary_source=None,
                posted_at=_parse_date(pubdate_el.text if pubdate_el is not None else None),
            )
            total += 1

        console.print(f"  [green]✓[/] We Work Remotely: {total} jobs fetched")
