"""Analytics and data interpretation engine.

Aggregates stored job listing data into insights: top skills/tags,
salary distributions, most active companies, and trends over time.
"""

from collections import Counter
from typing import List
from supabase import Client


def get_top_tags(db: Client, limit: int = 20) -> List[dict]:
    """Get the most common tags/skills across active listings."""
    result = db.table("job_listings").select("tags").eq("is_active", True).execute()

    counter = Counter()
    for row in result.data:
        tags = row.get("tags") or []
        if isinstance(tags, list):
            for tag in tags:
                counter[tag.lower()] += 1

    return [{"tag": tag, "count": count} for tag, count in counter.most_common(limit)]


def get_salary_distribution(db: Client) -> List[dict]:
    """Get salary range distribution in buckets."""
    result = db.table("job_listings").select(
        "salary_min, salary_max"
    ).eq("is_active", True).or_("salary_min.not.is.null,salary_max.not.is.null").execute()

    buckets = {
        "0-50k": 0, "50k-80k": 0, "80k-120k": 0,
        "120k-160k": 0, "160k-200k": 0, "200k+": 0,
    }

    for row in result.data:
        sal = row.get("salary_min") or row.get("salary_max") or 0
        if sal < 50000:
            buckets["0-50k"] += 1
        elif sal < 80000:
            buckets["50k-80k"] += 1
        elif sal < 120000:
            buckets["80k-120k"] += 1
        elif sal < 160000:
            buckets["120k-160k"] += 1
        elif sal < 200000:
            buckets["160k-200k"] += 1
        else:
            buckets["200k+"] += 1

    return [{"range": k, "count": v} for k, v in buckets.items()]


def get_top_companies(db: Client, limit: int = 15) -> List[dict]:
    """Get companies with the most active listings."""
    result = db.table("job_listings").select("company").eq("is_active", True).execute()

    counter = Counter()
    for row in result.data:
        counter[row["company"]] += 1

    return [{"company": c, "count": n} for c, n in counter.most_common(limit)]


def get_scrape_history(db: Client, limit: int = 30) -> List[dict]:
    """Get recent scrape run history for trend charts."""
    result = db.table("scrape_runs").select(
        "source, started_at, quality_score, total_count, added_count, removed_count, retained_count"
    ).eq("status", "completed").order("started_at", desc=True).limit(limit).execute()

    return [
        {
            "source": r["source"], "date": r["started_at"],
            "quality_score": r["quality_score"], "total": r["total_count"],
            "added": r["added_count"], "removed": r["removed_count"],
            "retained": r["retained_count"],
        }
        for r in result.data
    ]


def get_sources_breakdown(db: Client) -> List[dict]:
    """Get listing counts per source."""
    result = db.table("job_listings").select("source").eq("is_active", True).execute()

    counter = Counter()
    for row in result.data:
        counter[row["source"]] += 1

    return [{"source": s, "count": n} for s, n in counter.items()]
