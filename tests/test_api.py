"""
API smoke tests using FastAPI's TestClient.
These hit the real database — run after `make seed`.
Skip gracefully if DB is unavailable.
"""
import pytest
from fastapi.testclient import TestClient

from db import check_connection


@pytest.fixture(scope="module")
def client():
    if not check_connection():
        pytest.skip("Database not available — run `make db-up && make seed` first")
    from api.main import app
    return TestClient(app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["database"] == "connected"


def test_root(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "JobSignals" in r.json()["product"]


def test_list_jobs_default(client):
    r = client.get("/v1/jobs")
    assert r.status_code == 200
    body = r.json()
    assert "data" in body
    assert "meta" in body
    assert isinstance(body["data"], list)


def test_list_jobs_filter_family(client):
    r = client.get("/v1/jobs?title_family=Data Engineering&page_size=5")
    assert r.status_code == 200
    jobs = r.json()["data"]
    for job in jobs:
        assert job["title_family"] == "Data Engineering"


def test_list_jobs_filter_modality(client):
    r = client.get("/v1/jobs?modality=remote&page_size=5")
    assert r.status_code == 200
    for job in r.json()["data"]:
        assert job["location"]["modality"] == "remote"


def test_get_job_not_found(client):
    r = client.get("/v1/jobs/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404


def test_skill_trends(client):
    r = client.get("/v1/skills/trends?window=30&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body["data"], list)
    if body["data"]:
        trend = body["data"][0]
        assert "skill_name" in trend
        assert "posting_share" in trend
        assert "rank" in trend


def test_skill_taxonomy(client):
    r = client.get("/v1/skills/taxonomy")
    assert r.status_code == 200
    assert len(r.json()["data"]) > 10


def test_list_companies(client):
    r = client.get("/v1/companies?page_size=5")
    assert r.status_code == 200
    assert len(r.json()["data"]) > 0


def test_company_signals(client):
    # Get first available company
    companies = client.get("/v1/companies?page_size=1").json()["data"]
    if not companies:
        pytest.skip("No companies in DB")
    cid = companies[0]["company_id"]
    r = client.get(f"/v1/companies/{cid}/signals?window=90")
    assert r.status_code == 200
    data = r.json()["data"]
    assert "hiring_velocity_score" in data
    assert "top_skills" in data


def test_salary_benchmark(client):
    r = client.get(
        "/v1/salaries/benchmark"
        "?title_family=Data Engineering&seniority=senior&country=US"
    )
    # May return 404 if sample size < 10, that's valid behaviour
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        data = r.json()["data"]
        assert "percentile_50" in data
        assert data["sample_size"] >= 10


def test_salary_benchmark_invalid_family(client):
    r = client.get("/v1/salaries/benchmark?title_family=Nonsense&seniority=senior&country=US")
    assert r.status_code == 422


def test_stats_shape(client):
    r = client.get("/v1/stats")
    assert r.status_code == 200
    s = r.json()
    assert "total_postings" in s
    assert "active_postings" in s
    assert "unique_skills" in s
    assert "companies_hiring" in s
    assert "role_families" in s
    assert "sources" in s
    assert isinstance(s["sources"], dict)
    assert s["total_postings"] >= 0
    assert 0.0 <= s["salary_coverage_pct"] <= 100.0


def test_stats_no_seed_data(client):
    """seed postings must be excluded from /v1/stats counts."""
    r = client.get("/v1/stats")
    assert r.status_code == 200
    sources = r.json()["sources"]
    assert "seed" not in sources


def test_stats_known_sources(client):
    """source_platform values must all be recognised names, not 'other'."""
    r = client.get("/v1/stats")
    assert r.status_code == 200
    unknown = [s for s in r.json()["sources"] if s == "other"]
    assert unknown == [], f"'other' source still present: {r.json()['sources']}"


def test_stats_country_filter(client):
    """?country=US should return <= global totals, ?country=XX should return 0."""
    r_global = client.get("/v1/stats")
    assert r_global.status_code == 200
    global_total = r_global.json()["total_postings"]

    r_us = client.get("/v1/stats?country=US")
    assert r_us.status_code == 200
    us_total = r_us.json()["total_postings"]
    assert us_total <= global_total, "US count should not exceed global"

    r_xx = client.get("/v1/stats?country=XX")
    assert r_xx.status_code == 200
    assert r_xx.json()["total_postings"] == 0, "Unknown country code should return 0"


def test_jobs_country_param(client):
    """?country=DE should only return DE postings; ?country=IN returns 0 (no IN data yet)."""
    r_de = client.get("/v1/jobs?country=DE&page_size=5")
    assert r_de.status_code == 200
    jobs = r_de.json()["data"]
    for job in jobs:
        assert job.get("location", {}).get("country") == "DE", \
            f"Expected DE country, got {job.get('location', {}).get('country')}"

    r_in = client.get("/v1/jobs?country=IN&page_size=5")
    assert r_in.status_code == 200
    # IN data only present after running Adzuna IN ingestion
    # Just confirm no non-IN results sneak through
    for job in r_in.json()["data"]:
        assert job.get("location", {}).get("country") == "IN"


def test_jobs_location_city_search(client):
    """?location= should search by city (free text), not country code."""
    r = client.get("/v1/jobs?location=New+York&page_size=5")
    assert r.status_code == 200
    # Should not error; results may be 0 or more


def test_insights_get(client):
    r = client.get("/v1/insights?q=What+are+the+top+skills&window=30")
    # 200 if GROQ_API_KEY set and reachable; 503 if key missing; 502 if LLM unreachable
    assert r.status_code in (200, 502, 503)
    if r.status_code == 200:
        data = r.json()["data"]
        assert "analysis" in data
        assert "question" in data
        assert len(data["analysis"]) > 10
