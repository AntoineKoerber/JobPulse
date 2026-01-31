"""RemoteOK scraping strategy.

Fetches job listings from the RemoteOK public JSON API.
Implements the BaseScrapeStrategy interface with RemoteOK-specific
field mapping and response parsing.
"""

import httpx
import logging
from typing import List
from src.scraper.base_strategy import BaseScrapeStrategy
from src.scraper.humanizer import Humanizer
from src.api.schemas import RawJobListing

logger = logging.getLogger(__name__)


class RemoteOKStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping RemoteOK job listings."""

    async def fetch(self) -> List[RawJobListing]:
        base_url = self.config.get("base_url", "https://remoteok.com/api")
        headers = self.get_headers()
        humanizer = Humanizer(base_delay=self.get_rate_limit())

        await humanizer.delay()

        async with httpx.AsyncClient(timeout=30.0) as client:
            logger.info("Fetching from RemoteOK: %s", base_url)
            response = await client.get(base_url, headers=headers)
            response.raise_for_status()
            data = response.json()

        # RemoteOK returns an array where first element is a legal notice
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and "legal" in str(data[0]).lower():
                data = data[1:]

        field_map = self.get_field_map()
        listings = []

        for item in data:
            if not isinstance(item, dict):
                continue

            ext_id = str(item.get(field_map.get("id", "id"), ""))
            if not ext_id:
                continue

            tags = item.get(field_map.get("tags", "tags"), [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            salary_min = item.get("salary_min")
            salary_max = item.get("salary_max")

            listings.append(RawJobListing(
                external_id=ext_id,
                source="remoteok",
                title=item.get(field_map.get("title", "position"), ""),
                company=item.get(field_map.get("company", "company"), ""),
                location=item.get(field_map.get("location", "location"), "Remote"),
                salary_min=int(salary_min) if salary_min else None,
                salary_max=int(salary_max) if salary_max else None,
                tags=tags if isinstance(tags, list) else [],
                url=item.get(field_map.get("url", "url"), ""),
                posted_at=item.get(field_map.get("posted_at", "date"), ""),
            ))

        logger.info("RemoteOK: fetched %d listings", len(listings))
        return listings
