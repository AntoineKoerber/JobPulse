"""Analytics and data interpretation engine.

Aggregates stored job listing data into insights: top skills/tags,
salary distributions, most active companies, and trends over time.
"""

import json
import aiosqlite
from collections import Counter
from typing import List, Dict


async def get_top_tags(db: aiosqlite.Connection, limit: int = 20) -> List[dict]:
    """Get the most common tags/skills across active listings."""
    cursor = await db.execute(
        "SELECT tags FROM job_listings WHERE is_active = 1"
    )
    rows = await cursor.fetchall()

    counter = Counter()
    for row in rows:
        tags = json.loads(row[0]) if row[0] else []
        for tag in tags:
            counter[tag.lower()] += 1

    return [{"tag": tag, "count": count} for tag, count in counter.most_common(limit)]


async def get_salary_distribution(db: aiosqlite.Connection) -> List[dict]:
    """Get salary range distribution in buckets."""
    cursor = await db.execute(
        """SELECT salary_min, salary_max FROM job_listings
           WHERE is_active = 1 AND (salary_min IS NOT NULL OR salary_max IS NOT NULL)"""
    )
    rows = await cursor.fetchall()

    buckets = {
        "0-50k": 0, "50k-80k": 0, "80k-120k": 0,
        "120k-160k": 0, "160k-200k": 0, "200k+": 0,
    }

    for row in rows:
        sal = row[0] or row[1] or 0
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


async def get_top_companies(db: aiosqlite.Connection, limit: int = 15) -> List[dict]:
    """Get companies with the most active listings."""
    cursor = await db.execute(
        """SELECT company, COUNT(*) as cnt FROM job_listings
           WHERE is_active = 1 GROUP BY company ORDER BY cnt DESC LIMIT ?""",
        (limit,),
    )
    rows = await cursor.fetchall()
    return [{"company": row[0], "count": row[1]} for row in rows]


async def get_scrape_history(db: aiosqlite.Connection, limit: int = 30) -> List[dict]:
    """Get recent scrape run history for trend charts."""
    cursor = await db.execute(
        """SELECT source, started_at, quality_score, total_count,
                  added_count, removed_count, retained_count
           FROM scrape_runs WHERE status = 'completed'
           ORDER BY started_at DESC LIMIT ?""",
        (limit,),
    )
    rows = await cursor.fetchall()
    return [
        {
            "source": row[0], "date": row[1], "quality_score": row[2],
            "total": row[3], "added": row[4], "removed": row[5], "retained": row[6],
        }
        for row in rows
    ]


async def get_sources_breakdown(db: aiosqlite.Connection) -> List[dict]:
    """Get listing counts per source."""
    cursor = await db.execute(
        """SELECT source, COUNT(*) as cnt FROM job_listings
           WHERE is_active = 1 GROUP BY source"""
    )
    rows = await cursor.fetchall()
    return [{"source": row[0], "count": row[1]} for row in rows]
