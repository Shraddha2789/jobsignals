# JobSignals — MVP

Job market intelligence as a data product.
Local Postgres · Python pipelines · FastAPI · Seed data included.

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | latest | [docker.com](https://docker.com) |
| Python | ≥ 3.11 | [python.org](https://python.org) — check with `python3 --version` |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |

---

## Quick Start (5 minutes)

```bash
# 1. Clone / open in Cursor
cd jobsignals

# 2. Install Python deps + copy .env
make setup

# 3. Start PostgreSQL (Docker)
make db-up

# 4. Seed 400 realistic job postings + run aggregations
make seed

# 5. Start the API
make api
```

Then open:
- **API docs**: http://localhost:8000/docs
- **pgAdmin**:  http://localhost:5050  (email: `admin@jobsignals.local` / pw: `admin`)

---

## Project Structure

```
jobsignals/
├── db/
│   ├── schema.sql          ← PostgreSQL schema (all 5 tables)
│   └── connection.py       ← SQLAlchemy engine (swap DATABASE_URL for cloud)
│
├── ingestion/
│   ├── adapters/
│   │   ├── base.py         ← Abstract adapter interface
│   │   └── seed.py         ← Realistic dev data generator (400 postings, 40 companies)
│   └── models.py           ← Pydantic models for raw job data
│
├── pipeline/
│   ├── normalization/
│   │   ├── title_normalizer.py  ← Maps raw titles → family + seniority
│   │   └── skill_extractor.py   ← Rule-based skill extraction (60+ skills)
│   ├── deduplication.py    ← Content hash + source_id dedup
│   ├── aggregations.py     ← Builds skill_trends + company_signals
│   └── runner.py           ← Full ingestion → normalize → persist → aggregate
│
├── api/
│   ├── main.py             ← FastAPI app
│   ├── deps.py             ← DB dependency injection
│   ├── routers/
│   │   ├── jobs.py         ← GET /v1/jobs, GET /v1/jobs/{id}
│   │   ├── skills.py       ← GET /v1/skills/trends, GET /v1/skills/taxonomy
│   │   ├── companies.py    ← GET /v1/companies, GET /v1/companies/{id}/signals
│   │   └── salaries.py     ← GET /v1/salaries/benchmark
│   └── schemas/
│       └── responses.py    ← Pydantic response models
│
├── scripts/
│   ├── seed_data.py        ← One-shot seed CLI
│   ├── run_pipeline.py     ← One-shot pipeline CLI
│   └── scheduler.py        ← Cron scheduler (→ Airflow in Phase 2)
│
├── tests/
│   ├── test_pipeline.py    ← Unit tests (no DB required)
│   └── test_api.py         ← Integration tests (DB required)
│
├── docker-compose.yml
├── pyproject.toml
├── Makefile
└── .env.example
```

---

## API Reference

All responses use the envelope: `{ data, meta, errors }`

### Jobs
```
GET /v1/jobs
  ?q=             full-text search
  ?title_family=  Data Engineering | Data Science | ML Engineering | Software Engineering | Product Management
  ?modality=      remote | hybrid | onsite
  ?seniority=     junior | mid | senior | staff | principal
  ?salary_min=    integer (USD)
  ?skills=        comma-separated skill names (AND filter)
  ?page_size=     1–100 (default 20)
  ?cursor=        pagination cursor

GET /v1/jobs/{job_id}
```

### Skills
```
GET /v1/skills/trends
  ?title_family=  filter by role family
  ?country=       ISO 2-letter code (default US)
  ?window=        7 | 30 | 90 | 365 (days)
  ?limit=         1–100 (default 25)
  ?order_by=      posting_count | posting_share | mom_change

GET /v1/skills/taxonomy   — full skills ontology
```

### Companies
```
GET /v1/companies
  ?q=             name search
  ?industry=      filter by industry
  ?company_stage= startup | series_a | series_b | series_c | growth | public | enterprise

GET /v1/companies/{company_id}/signals
  ?window=        30 | 90 | 365 (days)
```

### Salaries
```
GET /v1/salaries/benchmark
  ?title_family=  (required)
  ?seniority=     (required)
  ?country=       ISO code (default US)
  ?company_stage= optional filter
  ?window_days=   30 | 90 | 365
```

---

## Development Commands

```bash
make setup      # install deps + copy .env
make db-up      # start postgres + pgadmin
make db-down    # stop containers
make db-reset   # wipe + recreate (re-run seed after)
make seed       # load 400 dev postings
make api        # start API on :8000
make scheduler  # start cron scheduler
make test       # run pytest
make lint       # ruff linter
```

---

## Migrating to Cloud (Phase 2)

The only change required is `DATABASE_URL` in `.env`:

```bash
# Neon (serverless Postgres)
DATABASE_URL=postgresql://user:pass@ep-xxx.neon.tech/jobsignals?sslmode=require

# BigQuery (requires additional driver)
DATABASE_URL=bigquery://project-id/dataset

# Snowflake
DATABASE_URL=snowflake://user:pass@account/database/schema
```

Everything else — schema, pipeline, API — stays the same.

---

## Adding a Real Source Adapter (Phase 2)

Create `ingestion/adapters/indeed.py`:

```python
from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

class IndeedAdapter(BaseAdapter):
    source_platform = "indeed"

    def fetch(self):
        # your scraping / API logic
        for raw_item in ...:
            yield RawJobPosting(
                source_id=raw_item["id"],
                source_platform="indeed",
                title_raw=raw_item["title"],
                ...
            )
```

Then register it in `pipeline/runner.py` alongside `SeedAdapter`.
The rest of the pipeline (normalization, dedup, persist, aggregate) handles it automatically.
