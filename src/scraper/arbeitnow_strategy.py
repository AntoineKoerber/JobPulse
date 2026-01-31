"""Arbeitnow scraping strategy.

Fetches job listings from the Arbeitnow public JSON API.
Supports pagination to collect all available listings.
"""

import httpx
import logging
from typing import List
from src.scraper.base_strategy import BaseScrapeStrategy
from src.scraper.humanizer import Humanizer
from src.api.schemas import RawJobListing

logger = logging.getLogger(__name__)


class ArbeitnowStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping Arbeitnow job listings."""

    async def fetch(self) -> List[RawJobListing]:
        base_url = self.config.get("base_url", "https://www.arbeitnow.com/api/job-board-api")
        headers = self.get_headers()
        humanizer = Humanizer(base_delay=self.get_rate_limit())
        field_map = self.get_field_map()

        listings = []
        page = 1
        max_pages = self.config.get("max_pages", 5)

        async with httpx.AsyncClient(timeout=30.0) as client:
            while page <= max_pages:
                await humanizer.delay()

                url = f"{base_url}?page={page}"
                logger.info("Fetching from Arbeitnow: %s", url)
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()

                items = data.get("data", [])
                if not items:
                    break

                for item in items:
                    ext_id = str(item.get(field_map.get("id", "slug"), ""))
                    if not ext_id:
                        continue

                    tags = item.get(field_map.get("tags", "tags"), [])
                    if isinstance(tags, str):
                        tags = [t.strip() for t in tags.split(",") if t.strip()]

                    posted = item.get(field_map.get("posted_at", "created_at"), "")
                    if isinstance(posted, (int, float)):
                        from datetime import datetime, timezone
                        posted = datetime.fromtimestamp(posted, tz=timezone.utc).isoformat()

                    listings.append(RawJobListing(
                        external_id=ext_id,
                        source="arbeitnow",
                        title=item.get(field_map.get("title", "title"), ""),
                        company=item.get(field_map.get("company", "company_name"), ""),
                        location=item.get(field_map.get("location", "location"), ""),
                        salary_raw=item.get("salary", None),
                        tags=tags if isinstance(tags, list) else [],
                        url=item.get(field_map.get("url", "url"), ""),
                        posted_at=str(posted) if posted else "",
                    ))

                # Check if there are more pages
                if not data.get("links", {}).get("next"):
                    break
                page += 1

        logger.info("Arbeitnow: fetched %d listings across %d pages", len(listings), page)
        return listings
