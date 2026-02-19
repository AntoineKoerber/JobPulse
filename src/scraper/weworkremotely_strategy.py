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

    # RSS feed categories
    CATEGORIES = [
        "programming",
        "design",
        "copywriting",
        "devops-sysadmin",
        "business-management-finance",
        "product",
        "customer-support",
        "sales-marketing",
    ]

    async def fetch(self) -> List[RawJobListing]:
        base_url = self.config.get("base_url", "https://weworkremotely.com/categories")
        headers = self.get_headers()
        humanizer = Humanizer(base_delay=self.get_rate_limit())

        listings = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for category in self.CATEGORIES:
                await humanizer.delay()

                feed_url = f"{base_url}/{category}.rss"
                logger.info("Fetching WWR category: %s", category)

                try:
                    response = await client.get(feed_url, headers=headers)
                    response.raise_for_status()

                    # Parse RSS XML
                    root = ET.fromstring(response.text)

                    for item in root.findall(".//item"):
                        listing = self._parse_item(item, category)
                        if listing:
                            listings.append(listing)

                except Exception as e:
                    logger.warning("Failed to fetch WWR category %s: %s", category, e)
                    continue

        # Deduplicate by external_id
        seen = set()
        unique_listings = []
        for listing in listings:
            if listing.external_id not in seen:
                seen.add(listing.external_id)
                unique_listings.append(listing)

        logger.info("WeWorkRemotely: fetched %d unique listings", len(unique_listings))
        return unique_listings

    def _parse_item(self, item: ET.Element, category: str) -> RawJobListing | None:
        """Parse a single RSS item into a RawJobListing."""
        try:
            title_elem = item.find("title")
            link_elem = item.find("link")
            pubdate_elem = item.find("pubDate")

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

            # Map category to tags
            category_tags = {
                "programming": ["Engineering", "Software"],
                "design": ["Design", "UI/UX"],
                "copywriting": ["Writing", "Content"],
                "devops-sysadmin": ["DevOps", "SysAdmin"],
                "business-management-finance": ["Business", "Finance"],
                "product": ["Product"],
                "customer-support": ["Support"],
                "sales-marketing": ["Sales", "Marketing"],
            }
            tags = category_tags.get(category, [])

            return RawJobListing(
                external_id=external_id,
                source="weworkremotely",
                title=title.strip(),
                company=company.strip(),
                location="Remote",
                tags=tags,
                url=url,
                posted_at=posted_at,
            )

        except Exception as e:
            logger.warning("Failed to parse WWR item: %s", e)
            return None
