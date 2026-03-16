"""Unit tests for pipeline normalization logic — no DB required."""
import pytest

from ingestion.adapters.seed import SeedAdapter
from pipeline.normalization.skill_extractor import extract_skills
from pipeline.normalization.title_normalizer import normalize_title


class TestTitleNormalizer:
    def test_senior_data_engineer(self):
        _, family, seniority = normalize_title("Senior Data Engineer")
        assert family == "Data Engineering"
        assert seniority == "senior"

    def test_staff_ml_engineer(self):
        _, family, seniority = normalize_title("Staff ML Engineer, Platform")
        assert family == "ML Engineering"
        assert seniority == "staff"

    def test_junior_pm(self):
        _, family, seniority = normalize_title("Associate Product Manager")
        assert family == "Product Management"
        assert seniority == "junior"

    def test_unknown_title_returns_other(self):
        _, family, _ = normalize_title("VP of Vibes")
        assert family == "Other"

    def test_cleans_noise_words(self):
        cleaned, _, _ = normalize_title("Senior Data Engineer - Remote USA")
        assert "Remote" not in cleaned
        assert "USA" not in cleaned


class TestSkillExtractor:
    def test_extracts_known_skills(self):
        desc = "Required skills: Python, dbt, Apache Spark. Nice to have: Kafka."
        skills = extract_skills(desc)
        names = {s.skill_name for s in skills}
        assert "Python" in names
        assert "dbt" in names
        assert "Apache Spark" in names
        assert "Apache Kafka" in names

    def test_required_vs_preferred(self):
        desc = "Required skills: Python. Nice to have: Rust."
        skills = extract_skills(desc)
        by_name = {s.skill_name: s for s in skills}
        assert by_name["Python"].is_required is True
        assert by_name["Rust"].is_required is False

    def test_no_false_positives_on_empty(self):
        skills = extract_skills("")
        assert skills == []

    def test_deduplication(self):
        desc = "Python is required. We use python3 for everything. Python experience needed."
        skills = extract_skills(desc)
        python_hits = [s for s in skills if s.skill_name == "Python"]
        assert len(python_hits) == 1


class TestSeedAdapter:
    def test_generates_correct_count(self):
        adapter = SeedAdapter(n_postings=10)
        postings = list(adapter.fetch())
        assert len(postings) == 10

    def test_all_postings_have_required_fields(self):
        adapter = SeedAdapter(n_postings=20)
        for p in adapter.fetch():
            assert p.source_id
            assert p.title_raw
            assert p.company_name
            assert p.description_raw
            assert p.source_platform == "seed"

    def test_salary_ranges_are_plausible(self):
        adapter = SeedAdapter(n_postings=50)
        salaries = [(p.salary_min, p.salary_max) for p in adapter.fetch() if p.salary_min]
        for lo, hi in salaries:
            assert lo >= 50_000
            assert hi <= 600_000
            assert hi >= lo
