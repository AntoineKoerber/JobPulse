"""Standalone daily scrape script for GitHub Actions.

Runs the full scrape pipeline (fetch → normalize → validate → change detect →
stability track → upsert) for all configured sources, then exits.
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from src.db.database import get_db
from src.api.schemas import JobListing
from src.scraper.strategy_factory import create_strategy
from src.pipeline.normalizer import (
    normalize_title, normalize_company, normalize_location,
    extract_salary, normalize_tags,
)
from src.pipeline.validator import score_listing, score_scrape, should_reject
from src.pipeline.change_detector import detect_changes, build_change_summary
from src.resilience.stability_tracker import update_stability
from src.resilience.fallback import get_last_successful_scrape, record_fallback_usage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("daily_scrape")

SOURCES = ["remoteok", "arbeitnow", "jobicy"]


async def run_scrape():
    results = {}
    db = get_db()

    for source_name in SOURCES:
        try:
            now = datetime.now(timezone.utc).isoformat()

            run_result = db.table("scrape_runs").insert({
                "source": source_name,
                "started_at": now,
                "status": "running",
            }).execute()
            run_id = run_result.data[0]["id"]

            strategy = create_strategy(source_name)
            raw_listings = await strategy.fetch()
            logger.info("%s: fetched %d raw listings", source_name, len(raw_listings))

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

            mean_score, issues = score_scrape(normalized)
            if issues:
                logger.warning("%s quality issues: %s", source_name, issues)

            if should_reject(mean_score):
                logger.warning("%s rejected (score %.1f), using fallback", source_name, mean_score)
                fallback = get_last_successful_scrape(db, source_name)
                record_fallback_usage(db, source_name, f"Score {mean_score}")
                results[source_name] = {"status": "fallback", "score": mean_score}
                continue

            prev_result = db.table("job_listings").select(
                "external_id"
            ).eq("source", source_name).eq("is_active", True).execute()
            previous_ids = {r["external_id"] for r in prev_result.data}
            current_ids = {l.external_id for l in normalized}
            changes = detect_changes(previous_ids, current_ids)
            summary = build_change_summary(changes)

            stability = update_stability(db, source_name, current_ids)

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
                "score": mean_score,
                "total": summary["total_count"],
                "added": summary["added_count"],
                "removed": len(stability["confirmed_removals"]),
            }
            logger.info("%s: completed — %s", source_name, results[source_name])

        except Exception as e:
            logger.error("%s: failed — %s", source_name, e, exc_info=True)
            results[source_name] = {"status": "failed", "error": str(e)}

    return results


def main():
    logger.info("Starting daily scrape for sources: %s", SOURCES)
    results = asyncio.run(run_scrape())
    logger.info("Scrape complete: %s", results)

    failed = [s for s, r in results.items() if r["status"] == "failed"]
    if failed:
        logger.error("Failed sources: %s", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
