"""API routes for JobPulse.

Provides endpoints for triggering scrapes, polling results, querying
stored listings, and retrieving trend/insight data.
"""

import json
import logging
from datetime import datetime, timezone
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
    db = get_db()

    for source_name in sources:
        try:
            now = datetime.now(timezone.utc).isoformat()

            # Record scrape run start
            run_result = db.table("scrape_runs").insert({
                "source": source_name,
                "started_at": now,
                "status": "running",
            }).execute()
            run_id = run_result.data[0]["id"]

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
                fallback = get_last_successful_scrape(db, source_name)
                record_fallback_usage(db, source_name, f"Score {mean_score}")
                results[source_name] = {
                    "status": "fallback",
                    "reason": f"Quality score {mean_score} below threshold",
                    "listings_count": len(fallback) if fallback else 0,
                }
                continue

            # Change detection
            prev_result = db.table("job_listings").select(
                "external_id"
            ).eq("source", source_name).eq("is_active", True).execute()
            previous_ids = {r["external_id"] for r in prev_result.data}
            current_ids = {l.external_id for l in normalized}
            changes = detect_changes(previous_ids, current_ids)
            summary = build_change_summary(changes)

            # Stability tracking
            stability = update_stability(db, source_name, current_ids)

            # Upsert listings
            now_ts = datetime.now(timezone.utc).isoformat()
            for listing in normalized:
                existing = db.table("job_listings").select("id").eq(
                    "external_id", listing.external_id
                ).eq("source", listing.source).execute()

                row = {
                    "external_id": listing.external_id,
                    "source": listing.source,
                    "title": listing.title,
                    "company": listing.company,
                    "location": listing.location,
                    "salary_min": listing.salary_min,
                    "salary_max": listing.salary_max,
                    "currency": listing.currency,
                    "tags": listing.tags,
                    "url": listing.url,
                    "posted_at": listing.posted_at,
                    "last_seen": now_ts,
                    "is_active": True,
                    "consecutive_misses": 0,
                    "quality_score": listing.quality_score,
                }

                if existing.data:
                    db.table("job_listings").update(row).eq(
                        "id", existing.data[0]["id"]
                    ).execute()
                else:
                    row["first_seen"] = now_ts
                    db.table("job_listings").insert(row).execute()

            # Update scrape run as completed
            completed_at = datetime.now(timezone.utc).isoformat()
            db.table("scrape_runs").update({
                "completed_at": completed_at,
                "status": "completed",
                "quality_score": mean_score,
                "total_count": summary["total_count"],
                "added_count": summary["added_count"],
                "removed_count": summary["removed_count"],
                "retained_count": summary["retained_count"],
            }).eq("id", run_id).execute()

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
    db = get_db()

    query = db.table("job_listings").select(
        "id, external_id, source, title, company, location, "
        "salary_min, salary_max, currency, tags, url, posted_at, "
        "first_seen, last_seen, quality_score",
        count="exact",
    ).eq("is_active", True)

    if source:
        query = query.eq("source", source)
    if location:
        query = query.ilike("location", f"%{location}%")
    if role:
        query = query.or_(f"title.ilike.%{role}%,tags.cs.[\"{role}\"]")
    if salary_min:
        query = query.gte("salary_min", salary_min)

    offset = (page - 1) * limit
    result = query.order("last_seen", desc=True).range(offset, offset + limit - 1).execute()

    return {
        "total": result.count or 0,
        "page": page,
        "limit": limit,
        "listings": result.data,
    }


@router.get("/trends")
async def get_trends():
    """Get aggregated trend and insight data for the dashboard."""
    db = get_db()
    return {
        "scrape_history": insights.get_scrape_history(db),
        "top_tags": insights.get_top_tags(db),
        "salary_distribution": insights.get_salary_distribution(db),
        "top_companies": insights.get_top_companies(db),
        "sources_breakdown": insights.get_sources_breakdown(db),
    }


@router.get("/health")
async def health():
    recent_jobs = queue.list_recent(limit=10)
    active = sum(1 for j in recent_jobs if j["status"] in ("queued", "running"))
    return {"ok": True, "active_jobs": active}
