"""We Work Remotely scraping strategy.

Fetches job listings from We Work Remotely RSS feeds.
Parses multiple category feeds and combines results.
"""

import httpx
import logging
import xml.etree.ElementTree as ET
from typing import List
from src.scraper.base_strategy import BaseScrapeStrategy
from src.scraper.humanizer import Humanizer
from src.api.schemas import RawJobListing
import hashlib
import re

logger = logging.getLogger(__name__)


class WeWorkRemotelyStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping We Work Remotely job listings."""

    async def fetch(self) -> List[RawJobListing]:
        # Use the main RSS feed which is not blocked by Cloudflare
        feed_url = self.config.get("base_url", "https://weworkremotely.com/remote-jobs.rss")
        headers = self.get_headers()

        listings = []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            logger.info("Fetching WWR main RSS feed: %s", feed_url)

            try:
                response = await client.get(feed_url, headers=headers)
                response.raise_for_status()

                # Parse RSS XML
                root = ET.fromstring(response.text)

                for item in root.findall(".//item"):
                    listing = self._parse_item(item)
                    if listing:
                        listings.append(listing)

            except Exception as e:
                logger.error("Failed to fetch WWR feed: %s", e)

        logger.info("WeWorkRemotely: fetched %d listings", len(listings))
        return listings

    def _parse_item(self, item: ET.Element) -> RawJobListing | None:
        """Parse a single RSS item into a RawJobListing."""
        try:
            title_elem = item.find("title")
            link_elem = item.find("link")
            pubdate_elem = item.find("pubDate")
            category_elem = item.find("category")
            region_elem = item.find("region")

            if title_elem is None or link_elem is None:
                return None

            full_title = title_elem.text or ""
            url = link_elem.text or ""
            posted_at = pubdate_elem.text if pubdate_elem is not None else ""

            # WWR titles are formatted as "Company: Job Title"
            if ": " in full_title:
                company, title = full_title.split(": ", 1)
            else:
                company = "Unknown"
                title = full_title

            # Generate external ID from URL
            external_id = hashlib.md5(url.encode()).hexdigest()[:16]

            # Use category from RSS feed
            tags = []
            if category_elem is not None and category_elem.text:
                tags = [category_elem.text]

            # Location from region element
            location = "Remote"
            if region_elem is not None and region_elem.text:
                location = region_elem.text

            return RawJobListing(
                external_id=external_id,
                source="weworkremotely",
                title=title.strip(),
                company=company.strip(),
                location=location,
                tags=tags,
                url=url,
                posted_at=posted_at,
            )

        except Exception as e:
            logger.warning("Failed to parse WWR item: %s", e)
            return None
