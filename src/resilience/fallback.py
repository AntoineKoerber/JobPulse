"""Fallback system for failed or low-quality scrapes.

When a scrape's quality score falls below the rejection threshold,
the system returns the most recent successful scrape data instead
of serving bad data. Mirrors Kivaro's scraper_fallback.py pattern.
"""

import aiosqlite
import json
from typing import Optional, List
from datetime import datetime


async def get_last_successful_scrape(
    db: aiosqlite.Connection,
    source: str,
) -> Optional[List[dict]]:
    """Retrieve listings from the last successful scrape for a source.

    Returns None if no successful scrape exists.
    """
    cursor = await db.execute(
        """SELECT id, quality_score, completed_at FROM scrape_runs
           WHERE source = ? AND status = 'completed' AND quality_score >= 60
           ORDER BY completed_at DESC LIMIT 1""",
        (source,),
    )
    run = await cursor.fetchone()
    if not run:
        return None

    # Return all active listings for this source
    cursor = await db.execute(
        """SELECT external_id, title, company, location, salary_min,
                  salary_max, currency, tags, url, posted_at, quality_score
           FROM job_listings WHERE source = ? AND is_active = 1""",
        (source,),
    )
    rows = await cursor.fetchall()
    if not rows:
        return None

    return [
        {
            "external_id": r[0], "title": r[1], "company": r[2],
            "location": r[3], "salary_min": r[4], "salary_max": r[5],
            "currency": r[6], "tags": json.loads(r[7]) if r[7] else [],
            "url": r[8], "posted_at": r[9], "quality_score": r[10],
        }
        for r in rows
    ]


async def record_fallback_usage(
    db: aiosqlite.Connection,
    source: str,
    reason: str,
):
    """Log when a fallback was used (stored as a failed scrape run)."""
    now = datetime.utcnow().isoformat()
    await db.execute(
        """INSERT INTO scrape_runs (source, started_at, completed_at, status, quality_score)
           VALUES (?, ?, ?, 'fallback', 0)""",
        (source, now, now),
    )
    await db.commit()
