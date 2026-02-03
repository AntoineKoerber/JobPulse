"""Jobicy scraping strategy.

Fetches remote job listings from the Jobicy public JSON API.
Jobicy provides salary data for ~50% of listings, making it
a valuable source for compensation analytics.
"""

import html
import httpx
import logging
from typing import List
from src.scraper.base_strategy import BaseScrapeStrategy
from src.scraper.humanizer import Humanizer
from src.api.schemas import RawJobListing

logger = logging.getLogger(__name__)


class JobicyStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping Jobicy job listings."""

    async def fetch(self) -> List[RawJobListing]:
        base_url = self.config.get("base_url", "https://jobicy.com/api/v2/remote-jobs")
        max_count = self.config.get("max_count", 100)
        headers = self.get_headers()
        humanizer = Humanizer(base_delay=self.get_rate_limit())
        field_map = self.get_field_map()

        await humanizer.delay()

        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{base_url}?count={max_count}"
            logger.info("Fetching from Jobicy: %s", url)
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

        items = data.get("jobs", [])
        listings = []

        for item in items:
            ext_id = str(item.get(field_map.get("id", "id"), ""))
            if not ext_id:
                continue

            tags = item.get(field_map.get("tags", "jobIndustry"), [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            # Jobicy includes jobType as a list (e.g. ["Full-Time"])
            job_types = item.get("jobType", [])
            if isinstance(job_types, list):
                tags = tags + job_types

            salary_min = item.get(field_map.get("salary_min", "salaryMin"))
            salary_max = item.get(field_map.get("salary_max", "salaryMax"))
            currency = item.get(field_map.get("currency", "salaryCurrency"), "")

            raw_title = item.get(field_map.get("title", "jobTitle"), "")
            raw_company = item.get(field_map.get("company", "companyName"), "")

            listings.append(RawJobListing(
                external_id=ext_id,
                source="jobicy",
                title=html.unescape(raw_title),
                company=html.unescape(raw_company),
                location=item.get(field_map.get("location", "jobGeo"), "Remote"),
                salary_min=int(salary_min) if salary_min else None,
                salary_max=int(salary_max) if salary_max else None,
                currency=currency if currency else None,
                tags=tags if isinstance(tags, list) else [],
                url=item.get(field_map.get("url", "url"), ""),
                posted_at=item.get(field_map.get("posted_at", "pubDate"), ""),
            ))

        logger.info("Jobicy: fetched %d listings", len(listings))
        return listings
