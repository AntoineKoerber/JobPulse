"""Quality validation system for scraped data.

Mirrors Kivaro's 0-100 scoring approach: each listing gets a quality score
based on field completeness, and the scrape-level score is the mean.
Scrapes below threshold trigger retry or fallback.
"""

from typing import List, Tuple
from src.api.schemas import JobListing

# Thresholds mirror Kivaro's validation system
RETRY_THRESHOLD = 60   # Retry scrape if mean quality < 60
REJECT_THRESHOLD = 40  # Fall back to last good scrape if < 40


def score_listing(listing: JobListing) -> int:
    """Score an individual listing based on field completeness (0-100)."""
    score = 0

    if listing.title and len(listing.title) > 2:
        score += 25
    if listing.company and len(listing.company) > 1:
        score += 25
    if listing.url:
        score += 20
    if listing.location:
        score += 15
    if listing.salary_min or listing.salary_max:
        score += 15

    return score


def score_scrape(listings: List[JobListing]) -> Tuple[float, List[str]]:
    """Score an entire scrape run. Returns (mean_score, issues).

    Issues list describes what problems were found, useful for logging.
    """
    if not listings:
        return (0.0, ["No listings returned"])

    issues = []
    scores = [score_listing(l) for l in listings]
    mean = sum(scores) / len(scores)

    low_quality = [s for s in scores if s < 50]
    if len(low_quality) > len(scores) * 0.5:
        issues.append(f"{len(low_quality)}/{len(scores)} listings scored below 50")

    no_salary = sum(1 for l in listings if not l.salary_min and not l.salary_max)
    if no_salary == len(listings):
        issues.append("No listings have salary data")

    no_location = sum(1 for l in listings if not l.location)
    if no_location > len(listings) * 0.8:
        issues.append(f"{no_location}/{len(listings)} listings missing location")

    return (round(mean, 1), issues)


def should_retry(score: float) -> bool:
    return score < RETRY_THRESHOLD


def should_reject(score: float) -> bool:
    return score < REJECT_THRESHOLD
