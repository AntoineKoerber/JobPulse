"""PeoplePerHour scraping strategy.

Fetches gig listings from PeoplePerHour hourlies and projects RSS feeds.
"""

import asyncio
import hashlib
import logging
import xml.etree.ElementTree as ET
from typing import List

import httpx

from src.api.schemas import RawJobListing
from src.scraper.base_strategy import BaseScrapeStrategy

logger = logging.getLogger(__name__)


class PeoplePerHourStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping PeoplePerHour gig listings."""

    async def fetch(self) -> List[RawJobListing]:
        feed_urls = self.config.get("feed_urls", [])
        headers = self.get_headers()
        rate_limit = self.get_rate_limit()

        listings = []
        seen_ids: set = set()

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for i, url in enumerate(feed_urls):
                if i > 0:
                    await asyncio.sleep(rate_limit)
                logger.info("Fetching PeoplePerHour feed: %s", url)
                try:
                    text = await self._http_get_text(client, url, headers=headers)
                    root = ET.fromstring(text)
                    for item in root.findall(".//item"):
                        listing = self._parse_item(item)
                        if listing and listing.external_id not in seen_ids:
                            seen_ids.add(listing.external_id)
                            listings.append(listing)
                except Exception as e:
                    logger.error("Failed to fetch PeoplePerHour feed %s: %s", url, e)

        logger.info("PeoplePerHour: fetched %d listings", len(listings))
        return listings

    def _parse_item(self, item: ET.Element) -> RawJobListing | None:
        try:
            title_elem = item.find("title")
            link_elem = item.find("link")
            pubdate_elem = item.find("pubDate")
            desc_elem = item.find("description")
            category_elem = item.find("category")

            if title_elem is None or link_elem is None:
                return None

            title = (title_elem.text or "").strip()
            url = (link_elem.text or "").strip()
            if not title or not url:
                return None

            external_id = hashlib.md5(url.encode()).hexdigest()[:16]
            posted_at = pubdate_elem.text if pubdate_elem is not None else ""

            tags = []
            if category_elem is not None and category_elem.text:
                tags = [category_elem.text.strip()]

            salary_raw = desc_elem.text if desc_elem is not None else None

            return RawJobListing(
                external_id=external_id,
                source="peopleperhour",
                title=title,
                company="PeoplePerHour",
                location="Remote",
                tags=tags,
                url=url,
                posted_at=posted_at,
                salary_raw=salary_raw,
            )
        except Exception as e:
            logger.warning("Failed to parse PeoplePerHour item: %s", e)
            return None
