"""Hacker News Jobs scraping strategy.

Fetches job/gig listings from HN Jobs via hnrss.org.
"""

import hashlib
import logging
import xml.etree.ElementTree as ET
from typing import List

import httpx

from src.api.schemas import RawJobListing
from src.scraper.base_strategy import BaseScrapeStrategy

logger = logging.getLogger(__name__)


class HNFreelanceStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping HN Jobs listings."""

    async def fetch(self) -> List[RawJobListing]:
        feed_url = self.config.get("base_url", "https://hnrss.org/jobs")
        headers = self.get_headers()

        listings = []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            logger.info("Fetching HN Jobs RSS feed")
            try:
                text = await self._http_get_text(client, feed_url, headers=headers)
                root = ET.fromstring(text)
                for item in root.findall(".//item"):
                    listing = self._parse_item(item)
                    if listing:
                        listings.append(listing)
            except Exception as e:
                logger.error("Failed to fetch HN Jobs feed: %s", e)

        logger.info("HNFreelance: fetched %d listings", len(listings))
        return listings

    def _parse_item(self, item: ET.Element) -> RawJobListing | None:
        try:
            title_elem = item.find("title")
            link_elem = item.find("link")
            pubdate_elem = item.find("pubDate")
            desc_elem = item.find("description")

            if title_elem is None or link_elem is None:
                return None

            title = (title_elem.text or "").strip()
            url = (link_elem.text or "").strip()

            if not title or not url:
                return None

            external_id = hashlib.md5(url.encode()).hexdigest()[:16]
            posted_at = pubdate_elem.text if pubdate_elem is not None else ""
            return RawJobListing(
                external_id=external_id,
                source="hnfreelance",
                title=title,
                company="Hacker News",
                location="Remote",
                tags=["tech", "contract"],
                url=url,
                posted_at=posted_at,
            )
        except Exception as e:
            logger.warning("Failed to parse HN Jobs item: %s", e)
            return None
