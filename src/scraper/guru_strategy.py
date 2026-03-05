"""Guru.com scraping strategy.

Fetches gig listings from Guru.com's single RSS feed.
"""

import hashlib
import logging
import xml.etree.ElementTree as ET
from typing import List

import httpx

from src.api.schemas import RawJobListing
from src.scraper.base_strategy import BaseScrapeStrategy

logger = logging.getLogger(__name__)


class GuruStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping Guru.com gig listings."""

    async def fetch(self) -> List[RawJobListing]:
        feed_url = self.config.get("base_url", "https://www.guru.com/jobs/rss/")
        headers = self.get_headers()

        listings = []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            logger.info("Fetching Guru RSS feed: %s", feed_url)
            try:
                text = await self._http_get_text(client, feed_url, headers=headers)
                root = ET.fromstring(text)
                for item in root.findall(".//item"):
                    listing = self._parse_item(item)
                    if listing:
                        listings.append(listing)
            except Exception as e:
                logger.error("Failed to fetch Guru feed: %s", e)

        logger.info("Guru: fetched %d listings", len(listings))
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

            # Strip " | Category" suffix common in Guru titles
            if " | " in title:
                title = title.rsplit(" | ", 1)[0].strip()

            external_id = hashlib.md5(url.encode()).hexdigest()[:16]
            posted_at = pubdate_elem.text if pubdate_elem is not None else ""

            tags = []
            if category_elem is not None and category_elem.text:
                tags = [category_elem.text.strip()]

            salary_raw = desc_elem.text if desc_elem is not None else None

            return RawJobListing(
                external_id=external_id,
                source="guru",
                title=title,
                company="Guru",
                location="Remote",
                tags=tags,
                url=url,
                posted_at=posted_at,
                salary_raw=salary_raw,
            )
        except Exception as e:
            logger.warning("Failed to parse Guru item: %s", e)
            return None
