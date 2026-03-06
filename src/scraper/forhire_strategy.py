"""Reddit r/forhire scraping strategy.

Fetches gig/hiring posts from Reddit's r/forhire community via RSS.
Posts follow the convention: [HIRING] or [FOR HIRE] Title | Budget | Details
"""

import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from typing import List

import httpx

from src.api.schemas import RawJobListing
from src.scraper.base_strategy import BaseScrapeStrategy

logger = logging.getLogger(__name__)

NS = {"atom": "http://www.w3.org/2005/Atom"}


class ForHireStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping Reddit r/forhire gig listings."""

    async def fetch(self) -> List[RawJobListing]:
        feed_url = self.config.get("base_url", "https://www.reddit.com/r/forhire/.rss?limit=100")
        headers = self.get_headers()

        listings = []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            logger.info("Fetching r/forhire RSS feed")
            try:
                text = await self._http_get_text(client, feed_url, headers=headers)
                root = ET.fromstring(text)
                for entry in root.findall("atom:entry", NS):
                    listing = self._parse_entry(entry)
                    if listing:
                        listings.append(listing)
            except Exception as e:
                logger.error("Failed to fetch r/forhire feed: %s", e)

        logger.info("ForHire: fetched %d listings", len(listings))
        return listings

    def _parse_entry(self, entry: ET.Element) -> RawJobListing | None:
        try:
            title_elem = entry.find("atom:title", NS)
            link_elem = entry.find("atom:link", NS)
            updated_elem = entry.find("atom:updated", NS)
            id_elem = entry.find("atom:id", NS)

            if title_elem is None:
                return None

            title = (title_elem.text or "").strip()
            url = link_elem.get("href", "") if link_elem is not None else ""
            posted_at = updated_elem.text if updated_elem is not None else ""

            if not title or not url:
                return None

            # Only keep [HIRING] posts — skip [FOR HIRE] self-promotion
            if not title.upper().startswith("[HIRING]"):
                return None

            # Strip the [HIRING] prefix
            title = re.sub(r"^\[HIRING\]\s*", "", title, flags=re.IGNORECASE).strip()

            external_id = hashlib.md5(url.encode()).hexdigest()[:16]

            # Extract budget if present (e.g. "$50-100", "€200", "negotiable")
            salary_raw = title  # normalizer will attempt extraction

            return RawJobListing(
                external_id=external_id,
                source="forhire",
                title=title,
                company="Reddit r/forhire",
                location="Remote",
                tags=["freelance"],
                url=url,
                posted_at=posted_at,
                salary_raw=salary_raw,
            )
        except Exception as e:
            logger.warning("Failed to parse r/forhire entry: %s", e)
            return None
