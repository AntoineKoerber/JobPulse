"""Tests for the data normalization pipeline."""

import pytest
from src.pipeline.normalizer import (
    normalize_title, normalize_company, normalize_location,
    extract_salary, normalize_tags, strip_html,
)


class TestNormalizeTitle:
    def test_expands_abbreviations(self):
        assert normalize_title("Sr. Backend Dev") == "Senior Backend Developer"

    def test_preserves_acronyms(self):
        assert "API" in normalize_title("api engineer")
        assert "AWS" in normalize_title("aws solutions architect")

    def test_strips_html(self):
        assert normalize_title("<b>Software Engineer</b>") == "Software Engineer"

    def test_normalizes_whitespace(self):
        assert normalize_title("  Full  Stack   Dev  ") == "Full Stack Developer"


class TestNormalizeCompany:
    def test_strips_legal_suffix(self):
        assert normalize_company("Acme Inc.") == "Acme"
        assert normalize_company("Big Corp LLC") == "Big Corp"

    def test_preserves_clean_names(self):
        assert normalize_company("Google") == "Google"


class TestNormalizeLocation:
    def test_remote_variants(self):
        assert normalize_location("Remote") == "Remote"
        assert normalize_location("anywhere") == "Remote"
        assert normalize_location("Worldwide") == "Remote"

    def test_remote_with_region(self):
        assert normalize_location("Remote, US") == "Remote (US)"
        assert normalize_location("Remote (Europe)") == "Remote (Europe)"

    def test_none(self):
        assert normalize_location(None) is None

    def test_regular_location(self):
        assert normalize_location("San Francisco, CA") == "San Francisco, CA"


class TestExtractSalary:
    def test_numeric_values(self):
        assert extract_salary(salary_min=80000, salary_max=120000) == (80000, 120000, "USD")

    def test_k_suffix(self):
        assert extract_salary(raw="$80k - $120k") == (80000, 120000, "USD")

    def test_eur(self):
        mn, mx, cur = extract_salary(raw="60000 - 90000 EUR")
        assert cur == "EUR"

    def test_none(self):
        assert extract_salary() == (None, None, None)


class TestNormalizeTags:
    def test_deduplicates(self):
        assert normalize_tags(["Python", "python", "PYTHON"]) == ["python"]

    def test_string_input(self):
        assert normalize_tags("react, node, python") == ["react", "node", "python"]

    def test_empty(self):
        assert normalize_tags([]) == []
        assert normalize_tags(None) == []
