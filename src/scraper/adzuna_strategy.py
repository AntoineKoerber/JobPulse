"""Adzuna API scraping strategy.

Fetches job listings from Adzuna's public API.
Requires API credentials from https://developer.adzuna.com/

Environment variables:
- ADZUNA_APP_ID: Your Adzuna application ID
- ADZUNA_APP_KEY: Your Adzuna API key
"""

import os
import httpx
import logging
from typing import List
from src.scraper.base_strategy import BaseScrapeStrategy
from src.scraper.humanizer import Humanizer
from src.api.schemas import RawJobListing

logger = logging.getLogger(__name__)


class AdzunaStrategy(BaseScrapeStrategy):
    """Concrete strategy for scraping Adzuna job listings via their API."""

    # Countries to search (Adzuna has different endpoints per country)
    COUNTRIES = ["us", "gb", "de", "fr", "nl", "ch", "at", "au", "ca"]

    # Tech-related categories/keywords to search
    SEARCH_TERMS = [
        "software engineer",
        "developer",
        "data scientist",
        "product manager",
        "devops",
        "frontend",
        "backend",
        "full stack",
    ]

    async def fetch(self) -> List[RawJobListing]:
        app_id = os.environ.get("ADZUNA_APP_ID", self.config.get("app_id", ""))
        app_key = os.environ.get("ADZUNA_APP_KEY", self.config.get("app_key", ""))

        if not app_id or not app_key:
            logger.error("Adzuna API credentials not configured. Set ADZUNA_APP_ID and ADZUNA_APP_KEY")
            return []

        headers = self.get_headers()
        humanizer = Humanizer(base_delay=self.get_rate_limit())
        listings = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for country in self.COUNTRIES:
                for search_term in self.SEARCH_TERMS:
                    await humanizer.delay()

                    try:
                        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
                        params = {
                            "app_id": app_id,
                            "app_key": app_key,
                            "results_per_page": 50,
                            "what": search_term,
                            "content-type": "application/json",
                            "category": "it-jobs",
                        }

                        logger.info("Fetching Adzuna: %s - %s", country.upper(), search_term)
                        response = await client.get(url, params=params, headers=headers)

                        if response.status_code == 401:
                            logger.error("Adzuna API authentication failed. Check credentials.")
                            return listings

                        response.raise_for_status()
                        data = response.json()

                        for job in data.get("results", []):
                            listing = self._parse_job(job, country)
                            if listing:
                                listings.append(listing)

                    except httpx.HTTPStatusError as e:
                        logger.warning("Adzuna API error for %s/%s: %s", country, search_term, e)
                        continue
                    except Exception as e:
                        logger.warning("Failed to fetch Adzuna %s/%s: %s", country, search_term, e)
                        continue

        # Deduplicate by external_id
        seen = set()
        unique_listings = []
        for listing in listings:
            if listing.external_id not in seen:
                seen.add(listing.external_id)
                unique_listings.append(listing)

        logger.info("Adzuna: fetched %d unique listings", len(unique_listings))
        return unique_listings

    def _parse_job(self, job: dict, country: str) -> RawJobListing | None:
        """Parse a single Adzuna job result into a RawJobListing."""
        try:
            job_id = job.get("id")
            if not job_id:
                return None

            # Extract location
            location_data = job.get("location", {})
            location_parts = []
            if location_data.get("display_name"):
                location_parts.append(location_data["display_name"])
            location = ", ".join(location_parts) if location_parts else country.upper()

            # Extract salary
            salary_min = job.get("salary_min")
            salary_max = job.get("salary_max")

            # Convert to int if present
            if salary_min:
                salary_min = int(salary_min)
            if salary_max:
                salary_max = int(salary_max)

            # Determine currency based on country
            currency_map = {
                "us": "USD",
                "gb": "GBP",
                "de": "EUR",
                "fr": "EUR",
                "nl": "EUR",
                "ch": "CHF",
                "at": "EUR",
                "au": "AUD",
                "ca": "CAD",
            }
            currency = currency_map.get(country, "USD")

            # Extract tags from category
            tags = []
            if job.get("category", {}).get("label"):
                tags.append(job["category"]["label"])

            return RawJobListing(
                external_id=str(job_id),
                source="adzuna",
                title=job.get("title", ""),
                company=job.get("company", {}).get("display_name", "Unknown"),
                location=location,
                salary_min=salary_min,
                salary_max=salary_max,
                currency=currency,
                tags=tags,
                url=job.get("redirect_url", ""),
                posted_at=job.get("created", ""),
            )

        except Exception as e:
            logger.warning("Failed to parse Adzuna job: %s", e)
            return None
