"""API routes for JobPulse.

Provides endpoints for triggering scrapes, polling results, querying
stored listings, and retrieving trend/insight data.
"""

import json
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Query, HTTPException

from src.api.schemas import ScrapeRequest, JobListing
from src.db.database import get_db
from src.jobs import queue
from src.scraper.strategy_factory import create_strategy
from src.pipeline.normalizer import (
    normalize_title, normalize_company, normalize_location,
    extract_salary, normalize_tags,
)
from src.pipeline.validator import score_listing, score_scrape, should_reject
from src.pipeline.change_detector import detect_changes, build_change_summary
from src.resilience.stability_tracker import update_stability
from src.resilience.fallback import get_last_successful_scrape, record_fallback_usage
from src.analytics import insights

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


async def _run_scrape(sources: list) -> dict:
    """Core scrape orchestration — runs all sources sequentially."""
    results = {}

    for source_name in sources:
        db = await get_db()
        try:
            now = datetime.utcnow().isoformat()
            await db.execute(
                "INSERT INTO scrape_runs (source, started_at, status) VALUES (?, ?, 'running')",
                (source_name, now),
            )
            await db.commit()

            # Fetch raw listings via strategy
            strategy = create_strategy(source_name)
            raw_listings = await strategy.fetch()

            # Normalize
            normalized = []
            for raw in raw_listings:
                sal_min, sal_max, currency = extract_salary(
                    raw.salary_raw, raw.salary_min, raw.salary_max
                )
                listing = JobListing(
                    external_id=raw.external_id,
                    source=raw.source,
                    title=normalize_title(raw.title),
                    company=normalize_company(raw.company),
                    location=normalize_location(raw.location),
                    salary_min=sal_min,
                    salary_max=sal_max,
                    currency=currency,
                    tags=normalize_tags(raw.tags),
                    url=raw.url,
                    posted_at=raw.posted_at,
                )
                listing.quality_score = score_listing(listing)
                normalized.append(listing)

            # Validate scrape quality
            mean_score, issues = score_scrape(normalized)
            if issues:
                logger.warning("Quality issues for %s: %s", source_name, issues)

            if should_reject(mean_score):
                logger.warning("Scrape rejected (score %.1f), using fallback", mean_score)
                fallback = await get_last_successful_scrape(db, source_name)
                await record_fallback_usage(db, source_name, f"Score {mean_score}")
                results[source_name] = {
                    "status": "fallback",
                    "reason": f"Quality score {mean_score} below threshold",
                    "listings_count": len(fallback) if fallback else 0,
                }
                continue

            # Change detection
            cursor = await db.execute(
                "SELECT external_id FROM job_listings WHERE source = ? AND is_active = 1",
                (source_name,),
            )
            previous_ids = {row[0] for row in await cursor.fetchall()}
            current_ids = {l.external_id for l in normalized}
            changes = detect_changes(previous_ids, current_ids)
            summary = build_change_summary(changes)

            # Stability tracking
            stability = await update_stability(db, source_name, current_ids)

            # Upsert listings
            now_ts = datetime.utcnow().isoformat()
            for listing in normalized:
                tags_json = json.dumps(listing.tags)
                await db.execute(
                    """INSERT INTO job_listings
                       (external_id, source, title, company, location, salary_min,
                        salary_max, currency, tags, url, posted_at, first_seen,
                        last_seen, is_active, consecutive_misses, quality_score)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?)
                       ON CONFLICT(external_id, source) DO UPDATE SET
                        title=excluded.title, company=excluded.company,
                        location=excluded.location, salary_min=excluded.salary_min,
                        salary_max=excluded.salary_max, currency=excluded.currency,
                        tags=excluded.tags, url=excluded.url,
                        last_seen=excluded.last_seen, is_active=1,
                        consecutive_misses=0, quality_score=excluded.quality_score""",
                    (
                        listing.external_id, listing.source, listing.title,
                        listing.company, listing.location, listing.salary_min,
                        listing.salary_max, listing.currency, tags_json,
                        listing.url, listing.posted_at, now_ts, now_ts,
                        listing.quality_score,
                    ),
                )

            # Record scrape run
            completed_at = datetime.utcnow().isoformat()
            await db.execute(
                """UPDATE scrape_runs SET completed_at=?, status='completed',
                   quality_score=?, total_count=?, added_count=?,
                   removed_count=?, retained_count=?
                   WHERE source=? AND started_at=?""",
                (
                    completed_at, mean_score, summary["total_count"],
                    summary["added_count"], summary["removed_count"],
                    summary["retained_count"], source_name, now,
                ),
            )
            await db.commit()

            results[source_name] = {
                "status": "completed",
                "quality_score": mean_score,
                "total": summary["total_count"],
                "added": summary["added_count"],
                "removed": len(stability["confirmed_removals"]),
                "tentative_removals": len(stability["tentative_removals"]),
            }
        except Exception as e:
            logger.error("Scrape failed for %s: %s", source_name, e, exc_info=True)
            results[source_name] = {"status": "failed", "error": str(e)}
        finally:
            await db.close()

    return results


@router.post("/scrape")
async def start_scrape(request: ScrapeRequest):
    """Start an async scrape job. Returns job_id for polling."""
    job_id = await queue.enqueue(lambda: _run_scrape(request.sources))
    return {
        "job_id": job_id,
        "status": "queued",
        "poll_url": f"/api/scrape/{job_id}",
    }


@router.get("/scrape/{job_id}")
async def get_scrape_status(job_id: str):
    """Poll scrape job status."""
    status = queue.get_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@router.get("/jobs")
async def list_jobs(
    source: Optional[str] = None,
    location: Optional[str] = None,
    role: Optional[str] = None,
    salary_min: Optional[int] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
):
    """Query stored job listings with filters."""
    db = await get_db()
    try:
        conditions = ["is_active = 1"]
        params = []

        if source:
            conditions.append("source = ?")
            params.append(source)
        if location:
            conditions.append("location LIKE ?")
            params.append(f"%{location}%")
        if role:
            conditions.append("(title LIKE ? OR tags LIKE ?)")
            params.append(f"%{role}%")
            params.append(f"%{role}%")
        if salary_min:
            conditions.append("salary_min >= ?")
            params.append(salary_min)

        where = " AND ".join(conditions)
        offset = (page - 1) * limit

        # Get total count
        cursor = await db.execute(
            f"SELECT COUNT(*) FROM job_listings WHERE {where}", params
        )
        total = (await cursor.fetchone())[0]

        # Get page
        cursor = await db.execute(
            f"""SELECT id, external_id, source, title, company, location,
                       salary_min, salary_max, currency, tags, url, posted_at,
                       first_seen, last_seen, quality_score
                FROM job_listings WHERE {where}
                ORDER BY last_seen DESC LIMIT ? OFFSET ?""",
            params + [limit, offset],
        )
        rows = await cursor.fetchall()

        listings = [
            {
                "id": r[0], "external_id": r[1], "source": r[2], "title": r[3],
                "company": r[4], "location": r[5], "salary_min": r[6],
                "salary_max": r[7], "currency": r[8],
                "tags": json.loads(r[9]) if r[9] else [],
                "url": r[10], "posted_at": r[11], "first_seen": r[12],
                "last_seen": r[13], "quality_score": r[14],
            }
            for r in rows
        ]

        return {"total": total, "page": page, "limit": limit, "listings": listings}
    finally:
        await db.close()


@router.get("/trends")
async def get_trends():
    """Get aggregated trend and insight data for the dashboard."""
    db = await get_db()
    try:
        return {
            "scrape_history": await insights.get_scrape_history(db),
            "top_tags": await insights.get_top_tags(db),
            "salary_distribution": await insights.get_salary_distribution(db),
            "top_companies": await insights.get_top_companies(db),
            "sources_breakdown": await insights.get_sources_breakdown(db),
        }
    finally:
        await db.close()


@router.get("/health")
async def health():
    recent_jobs = queue.list_recent(limit=10)
    active = sum(1 for j in recent_jobs if j["status"] in ("queued", "running"))
    return {"ok": True, "active_jobs": active}
