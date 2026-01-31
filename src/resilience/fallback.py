"""Fallback system for failed or low-quality scrapes.

When a scrape's quality score falls below the rejection threshold,
the system returns the most recent successful scrape data instead
of serving bad data. Mirrors Kivaro's scraper_fallback.py pattern.
"""

from typing import Optional, List
from supabase import Client


def get_last_successful_scrape(
    db: Client,
    source: str,
) -> Optional[List[dict]]:
    """Retrieve listings from the last successful scrape for a source.

    Returns None if no successful scrape exists.
    """
    # Check if a successful run exists
    run = db.table("scrape_runs").select("id, quality_score, completed_at").eq(
        "source", source
    ).eq("status", "completed").gte("quality_score", 60).order(
        "completed_at", desc=True
    ).limit(1).execute()

    if not run.data:
        return None

    # Return all active listings for this source
    result = db.table("job_listings").select(
        "external_id, title, company, location, salary_min, salary_max, "
        "currency, tags, url, posted_at, quality_score"
    ).eq("source", source).eq("is_active", True).execute()

    if not result.data:
        return None

    return result.data


def record_fallback_usage(db: Client, source: str, reason: str):
    """Log when a fallback was used (stored as a failed scrape run)."""
    db.table("scrape_runs").insert({
        "source": source,
        "status": "fallback",
        "quality_score": 0,
    }).execute()
