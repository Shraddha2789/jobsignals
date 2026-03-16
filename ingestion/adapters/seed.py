"""
Seed data adapter — generates ~400 realistic job postings for local development.

Covers 5 role families, 40 companies, realistic salary bands,
and skill associations that match real-world job descriptions.
Replace this adapter with LinkedIn/Indeed adapters in Phase 2.
"""
from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

from faker import Faker

from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Companies ─────────────────────────────────────────────────────────────────

COMPANIES = [
    # Big tech / public
    {"name": "Stripe",      "domain": "stripe.com",        "industry": "Fintech",           "stage": "public",    "employees": "5000+",     "country": "US"},
    {"name": "Databricks",  "domain": "databricks.com",    "industry": "Data & AI",         "stage": "growth",    "employees": "1001-5000", "country": "US"},
    {"name": "Snowflake",   "domain": "snowflake.com",     "industry": "Cloud Data",        "stage": "public",    "employees": "5000+",     "country": "US"},
    {"name": "Confluent",   "domain": "confluent.io",      "industry": "Data Streaming",    "stage": "public",    "employees": "1001-5000", "country": "US"},
    {"name": "Figma",       "domain": "figma.com",         "industry": "Design Tools",      "stage": "public",    "employees": "1001-5000", "country": "US"},
    {"name": "Notion",      "domain": "notion.so",         "industry": "Productivity",      "stage": "series_c",  "employees": "201-1000",  "country": "US"},
    {"name": "Vercel",      "domain": "vercel.com",        "industry": "Developer Tools",   "stage": "series_c",  "employees": "201-1000",  "country": "US"},
    {"name": "Anthropic",   "domain": "anthropic.com",     "industry": "AI Research",       "stage": "series_c",  "employees": "201-1000",  "country": "US"},
    {"name": "dbt Labs",    "domain": "getdbt.com",        "industry": "Data Tools",        "stage": "series_b",  "employees": "201-1000",  "country": "US"},
    {"name": "Retool",      "domain": "retool.com",        "industry": "Developer Tools",   "stage": "series_c",  "employees": "201-1000",  "country": "US"},
    # Series B/C startups
    {"name": "Linear",      "domain": "linear.app",        "industry": "Productivity",      "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Brex",        "domain": "brex.com",          "industry": "Fintech",           "stage": "series_c",  "employees": "1001-5000", "country": "US"},
    {"name": "Ramp",        "domain": "ramp.com",          "industry": "Fintech",           "stage": "series_c",  "employees": "201-1000",  "country": "US"},
    {"name": "Loom",        "domain": "loom.com",          "industry": "Productivity",      "stage": "series_c",  "employees": "201-1000",  "country": "US"},
    {"name": "Hex",         "domain": "hex.tech",          "industry": "Data Tools",        "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Preset",      "domain": "preset.io",         "industry": "Data Tools",        "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Metaplane",   "domain": "metaplane.dev",     "industry": "Data Observability","stage": "series_a",  "employees": "11-50",     "country": "US"},
    {"name": "Hightouch",   "domain": "hightouch.com",     "industry": "Data Tools",        "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Fivetran",    "domain": "fivetran.com",      "industry": "Data Integration",  "stage": "growth",    "employees": "1001-5000", "country": "US"},
    {"name": "Monte Carlo", "domain": "montecarlodata.com","industry": "Data Observability","stage": "series_c",  "employees": "201-1000",  "country": "US"},
    # Series A
    {"name": "Cohere",      "domain": "cohere.com",        "industry": "AI/ML",             "stage": "series_c",  "employees": "201-1000",  "country": "US"},
    {"name": "Mistral AI",  "domain": "mistral.ai",        "industry": "AI Research",       "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Together AI", "domain": "together.ai",       "industry": "AI Infra",          "stage": "series_a",  "employees": "51-200",    "country": "US"},
    {"name": "Modal Labs",  "domain": "modal.com",         "industry": "AI Infra",          "stage": "series_a",  "employees": "11-50",     "country": "US"},
    {"name": "Turso",       "domain": "turso.tech",        "industry": "Developer Tools",   "stage": "series_a",  "employees": "11-50",     "country": "US"},
    # Enterprise
    {"name": "Palantir",    "domain": "palantir.com",      "industry": "Enterprise AI",     "stage": "public",    "employees": "5000+",     "country": "US"},
    {"name": "Twilio",      "domain": "twilio.com",        "industry": "Communications",    "stage": "public",    "employees": "5000+",     "country": "US"},
    {"name": "Datadog",     "domain": "datadoghq.com",     "industry": "Observability",     "stage": "public",    "employees": "5000+",     "country": "US"},
    {"name": "HashiCorp",   "domain": "hashicorp.com",     "industry": "DevOps",            "stage": "public",    "employees": "1001-5000", "country": "US"},
    {"name": "Elastic",     "domain": "elastic.co",        "industry": "Search & Analytics","stage": "public",    "employees": "1001-5000", "country": "US"},
    # YC-style startups
    {"name": "Baseten",     "domain": "baseten.co",        "industry": "ML Infra",          "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Inngest",     "domain": "inngest.com",       "industry": "Developer Tools",   "stage": "series_a",  "employees": "11-50",     "country": "US"},
    {"name": "Neon",        "domain": "neon.tech",         "industry": "Database",          "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Unstructured","domain": "unstructured.io",   "industry": "AI/ML",             "stage": "series_a",  "employees": "11-50",     "country": "US"},
    {"name": "Vectara",     "domain": "vectara.com",       "industry": "AI/ML",             "stage": "series_a",  "employees": "51-200",    "country": "US"},
    {"name": "Qdrant",      "domain": "qdrant.tech",       "industry": "Vector Database",   "stage": "series_a",  "employees": "51-200",    "country": "US"},
    {"name": "Weaviate",    "domain": "weaviate.io",       "industry": "Vector Database",   "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "LanceDB",     "domain": "lancedb.com",       "industry": "Vector Database",   "stage": "series_a",  "employees": "11-50",     "country": "US"},
    {"name": "Arize AI",    "domain": "arize.com",         "industry": "ML Observability",  "stage": "series_b",  "employees": "51-200",    "country": "US"},
    {"name": "Fiddler AI",  "domain": "fiddler.ai",        "industry": "ML Observability",  "stage": "series_b",  "employees": "51-200",    "country": "US"},
]

# ── Role family definitions ───────────────────────────────────────────────────

ROLE_FAMILIES = {
    "Data Engineering": {
        "titles": {
            "junior":    ["Junior Data Engineer", "Data Engineer I", "Associate Data Engineer"],
            "mid":       ["Data Engineer", "Data Engineer II", "Analytics Engineer"],
            "senior":    ["Senior Data Engineer", "Senior Analytics Engineer"],
            "staff":     ["Staff Data Engineer", "Lead Data Engineer"],
            "principal": ["Principal Data Engineer", "Distinguished Data Engineer"],
        },
        "skills_required": ["Python", "SQL", "dbt", "Apache Spark", "Airflow"],
        "skills_optional": ["Kafka", "BigQuery", "Snowflake", "Kubernetes", "Docker",
                            "Terraform", "Databricks", "Delta Lake", "Redis", "AWS", "GCP"],
        "salary_by_level": {
            "junior":    (90_000,  130_000),
            "mid":       (130_000, 170_000),
            "senior":    (165_000, 220_000),
            "staff":     (210_000, 270_000),
            "principal": (260_000, 340_000),
        },
    },
    "Data Science": {
        "titles": {
            "junior":    ["Junior Data Scientist", "Data Scientist I"],
            "mid":       ["Data Scientist", "Applied Scientist"],
            "senior":    ["Senior Data Scientist", "Senior Applied Scientist"],
            "staff":     ["Lead Data Scientist", "Staff Data Scientist"],
            "principal": ["Principal Data Scientist", "Distinguished Scientist"],
        },
        "skills_required": ["Python", "SQL", "pandas", "scikit-learn", "Statistics"],
        "skills_optional": ["TensorFlow", "PyTorch", "R", "Spark", "Databricks",
                            "A/B Testing", "Machine Learning", "NumPy", "Matplotlib", "Jupyter"],
        "salary_by_level": {
            "junior":    (95_000,  135_000),
            "mid":       (135_000, 175_000),
            "senior":    (170_000, 230_000),
            "staff":     (220_000, 285_000),
            "principal": (270_000, 360_000),
        },
    },
    "ML Engineering": {
        "titles": {
            "junior":    ["Junior ML Engineer", "ML Engineer I"],
            "mid":       ["ML Engineer", "Machine Learning Engineer", "MLOps Engineer"],
            "senior":    ["Senior ML Engineer", "Senior Machine Learning Engineer"],
            "staff":     ["Staff ML Engineer", "Lead ML Engineer"],
            "principal": ["Principal ML Engineer", "Distinguished ML Engineer"],
        },
        "skills_required": ["Python", "PyTorch", "Machine Learning", "Kubernetes", "Docker"],
        "skills_optional": ["TensorFlow", "CUDA", "MLflow", "Ray", "FastAPI", "Triton",
                            "AWS SageMaker", "Vertex AI", "Redis", "Kafka", "C++", "Rust"],
        "salary_by_level": {
            "junior":    (110_000, 155_000),
            "mid":       (155_000, 200_000),
            "senior":    (195_000, 260_000),
            "staff":     (250_000, 320_000),
            "principal": (310_000, 400_000),
        },
    },
    "Software Engineering": {
        "titles": {
            "junior":    ["Junior Software Engineer", "Software Engineer I", "Associate SWE"],
            "mid":       ["Software Engineer", "Software Engineer II", "Backend Engineer"],
            "senior":    ["Senior Software Engineer", "Senior Backend Engineer", "Senior SWE"],
            "staff":     ["Staff Engineer", "Staff Software Engineer"],
            "principal": ["Principal Engineer", "Distinguished Engineer"],
        },
        "skills_required": ["Python", "SQL", "REST APIs", "Docker", "Git"],
        "skills_optional": ["Go", "Java", "Rust", "TypeScript", "PostgreSQL", "Redis",
                            "Kubernetes", "AWS", "GCP", "GraphQL", "gRPC", "React",
                            "Kafka", "Terraform", "Elasticsearch"],
        "salary_by_level": {
            "junior":    (100_000, 145_000),
            "mid":       (145_000, 190_000),
            "senior":    (185_000, 245_000),
            "staff":     (235_000, 310_000),
            "principal": (295_000, 380_000),
        },
    },
    "Product Management": {
        "titles": {
            "junior":    ["Associate Product Manager", "APM"],
            "mid":       ["Product Manager", "PM"],
            "senior":    ["Senior Product Manager", "Senior PM"],
            "staff":     ["Group Product Manager", "Lead PM"],
            "principal": ["Director of Product", "Principal PM"],
        },
        "skills_required": ["Product Strategy", "Roadmapping", "Stakeholder Management", "Agile"],
        "skills_optional": ["SQL", "A/B Testing", "Analytics", "JIRA", "User Research",
                            "Data Analysis", "Python", "Figma", "Go-to-Market"],
        "salary_by_level": {
            "junior":    (95_000,  130_000),
            "mid":       (130_000, 170_000),
            "senior":    (165_000, 220_000),
            "staff":     (210_000, 270_000),
            "principal": (250_000, 330_000),
        },
    },
}

SENIORITY_WEIGHTS = {"junior": 0.15, "mid": 0.35, "senior": 0.30, "staff": 0.13, "principal": 0.07}
MODALITY_WEIGHTS  = {"remote": 0.45, "hybrid": 0.35, "onsite": 0.20}
CITIES = [
    ("San Francisco", "US"), ("New York", "US"), ("Seattle", "US"),
    ("Austin", "US"),        ("Boston", "US"),   ("Chicago", "US"),
    ("Los Angeles", "US"),   ("Denver", "US"),   ("Atlanta", "US"),
    ("Remote", "US"),
]

JOB_DESCRIPTION_TEMPLATES = [
    "We are looking for a {title} to join our {team} team. In this role you will design and build "
    "{artifact} to support {goal}. You will collaborate closely with {peer_team} to deliver "
    "high-quality data infrastructure. {skills_para}",
    "As a {title} at {company}, you will own {artifact} end-to-end. You have a strong background "
    "in {core_skill} and thrive in a fast-paced environment. {skills_para}",
    "{company} is hiring a {title} who is passionate about {domain}. You will {verb} {artifact} "
    "and work with a world-class team. {skills_para}",
]
TEAMS    = ["Platform", "Growth", "Core", "Infrastructure", "Analytics", "Product"]
ARTIFACTS= ["data pipelines", "ML systems", "APIs", "data models", "real-time streams",
            "feature stores", "dashboards", "data products"]
GOALS    = ["business decisions", "product growth", "customer insights", "operational efficiency"]
PEER_TEAMS = ["Product", "ML", "Engineering", "Analytics", "Design", "Finance"]
DOMAINS  = ["data engineering", "machine learning", "product analytics", "platform engineering"]
VERBS    = ["design", "build", "scale", "maintain", "own", "architect"]


def _skills_paragraph(required: list[str], optional: list[str]) -> str:
    req_str = ", ".join(required[:4])
    opt_str = ", ".join(optional[:5])
    return (
        f"Required skills: {req_str}. "
        f"Nice to have: {opt_str}. "
        "We value curiosity, ownership, and impact over years of experience."
    )


def _content_hash(company_name: str, title: str, city: str, date: datetime) -> str:
    raw = f"{company_name.lower()}|{title.lower()}|{city.lower()}|{date.date()}"
    return hashlib.sha256(raw.encode()).hexdigest()


class SeedAdapter(BaseAdapter):
    """
    Generates realistic job postings for local development.

    Usage:
        adapter = SeedAdapter(n_postings=400)
        for posting in adapter.fetch():
            ...
    """

    source_platform = "seed"

    def __init__(self, n_postings: int = 400) -> None:
        self.n_postings = n_postings

    def fetch(self) -> Iterator[RawJobPosting]:
        seniority_choices = list(SENIORITY_WEIGHTS.keys())
        seniority_weights = list(SENIORITY_WEIGHTS.values())
        modality_choices  = list(MODALITY_WEIGHTS.keys())
        modality_weights  = list(MODALITY_WEIGHTS.values())

        # Spread postings over the last 90 days
        now = datetime.now(tz=timezone.utc)

        for i in range(self.n_postings):
            company = random.choice(COMPANIES)
            family  = random.choice(list(ROLE_FAMILIES.keys()))
            config  = ROLE_FAMILIES[family]

            seniority = random.choices(seniority_choices, weights=seniority_weights)[0]
            title_raw = random.choice(config["titles"][seniority])
            modality  = random.choices(modality_choices, weights=modality_weights)[0]
            city, country = random.choice(CITIES)
            if modality == "remote":
                city, country = "Remote", "US"

            sal_low, sal_high = config["salary_by_level"][seniority]
            # ~30% of postings don't publish salary
            has_salary = random.random() > 0.30
            if has_salary:
                sal_min = random.randint(sal_low, sal_low + (sal_high - sal_low) // 2)
                sal_max = sal_min + random.randint(20_000, 60_000)
            else:
                sal_min = sal_max = None

            required_skills = config["skills_required"].copy()
            optional_skills = random.sample(config["skills_optional"], k=min(5, len(config["skills_optional"])))

            posted_at = now - timedelta(days=random.randint(0, 89))
            source_id = f"seed_{i:05d}"

            description = random.choice(JOB_DESCRIPTION_TEMPLATES).format(
                title=title_raw,
                team=random.choice(TEAMS),
                artifact=random.choice(ARTIFACTS),
                goal=random.choice(GOALS),
                peer_team=random.choice(PEER_TEAMS),
                company=company["name"],
                core_skill=random.choice(required_skills),
                domain=random.choice(DOMAINS),
                verb=random.choice(VERBS),
                skills_para=_skills_paragraph(required_skills, optional_skills),
            )

            yield RawJobPosting(
                source_id=source_id,
                source_platform="seed",
                source_url=f"https://jobs.{company['domain']}/posting/{source_id}",
                title_raw=title_raw,
                company_name=company["name"],
                company_domain=company["domain"],
                location_raw=f"{city}, {country}",
                location_city=city,
                location_country=country,
                work_modality=modality,
                employment_type="full_time",
                seniority_level=seniority,
                description_raw=description,
                salary_min=sal_min,
                salary_max=sal_max,
                salary_currency="USD",
                salary_source="posted" if has_salary else None,
                posted_at=posted_at,
            )
