"""Hacker News Jobs scraping strategy.

Fetches job/gig listings from HN Jobs via hnrss.org.
Enriches listings with scores and comment counts from the HN API.
"""

import asyncio
import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from typing import List

import httpx

from src.api.schemas import RawJobListing
from src.scraper.base_strategy import BaseScrapeStrategy

logger = logging.getLogger(__name__)

HN_API = "https://hacker-news.firebaseio.com/v0/item/{}.json"


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

            # Enrich with HN API scores
            await self._enrich_scores(client, listings)

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
                salary_raw=title,
            )
        except Exception as e:
            logger.warning("Failed to parse HN Jobs item: %s", e)
            return None

    @staticmethod
    def _extract_hn_id(url: str) -> str | None:
        """Extract HN item ID from a URL like https://news.ycombinator.com/item?id=12345."""
        m = re.search(r"id=(\d+)", url)
        return m.group(1) if m else None

    async def _fetch_hn_item(self, client: httpx.AsyncClient, hn_id: str) -> dict | None:
        try:
            resp = await client.get(HN_API.format(hn_id), timeout=10.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    async def _enrich_scores(self, client: httpx.AsyncClient, listings: List[RawJobListing]):
        """Fetch HN scores and comment counts for all listings."""
        # Build (index, hn_id) pairs
        tasks = []
        for i, listing in enumerate(listings):
            hn_id = self._extract_hn_id(listing.url)
            if hn_id:
                tasks.append((i, hn_id))

        if not tasks:
            return

        # Fetch in batches of 5 to be polite
        sem = asyncio.Semaphore(5)

        async def fetch_with_sem(hn_id):
            async with sem:
                return await self._fetch_hn_item(client, hn_id)

        results = await asyncio.gather(
            *[fetch_with_sem(hn_id) for _, hn_id in tasks],
            return_exceptions=True,
        )

        enriched = 0
        for (idx, _), result in zip(tasks, results):
            if isinstance(result, dict) and result:
                listings[idx].hn_score = result.get("score", 0)
                listings[idx].hn_comments = result.get("descendants", 0)
                enriched += 1

        logger.info("HN enrichment: %d/%d listings enriched with scores", enriched, len(tasks))
