"""Tests for the quality validation system."""

import pytest
from src.pipeline.validator import score_listing, score_scrape, should_retry, should_reject
from src.api.schemas import JobListing


def make_listing(**kwargs):
    defaults = {
        "external_id": "1", "source": "test", "title": "Engineer",
        "company": "Acme", "location": "Remote",
        "salary_min": 100000, "salary_max": 150000,
        "url": "https://example.com/job/1",
    }
    defaults.update(kwargs)
    return JobListing(**defaults)


class TestScoreListing:
    def test_full_listing(self):
        listing = make_listing()
        assert score_listing(listing) == 100

    def test_missing_salary(self):
        listing = make_listing(salary_min=None, salary_max=None)
        assert score_listing(listing) == 85

    def test_missing_location_and_salary(self):
        listing = make_listing(location=None, salary_min=None, salary_max=None)
        assert score_listing(listing) == 70

    def test_empty_listing(self):
        listing = JobListing(external_id="1", source="test", title="", company="")
        assert score_listing(listing) == 0


class TestScoreScrape:
    def test_high_quality(self):
        listings = [make_listing() for _ in range(10)]
        score, issues = score_scrape(listings)
        assert score == 100.0
        assert len(issues) == 0

    def test_empty_scrape(self):
        score, issues = score_scrape([])
        assert score == 0.0
        assert "No listings" in issues[0]


class TestThresholds:
    def test_retry(self):
        assert should_retry(59.0) is True
        assert should_retry(60.0) is False

    def test_reject(self):
        assert should_reject(39.0) is True
        assert should_reject(40.0) is False
