"""Abstract base strategy for job board scrapers.

Mirrors Kivaro's strategy pattern: each job board gets its own concrete
strategy class that implements the fetch() method. The base class provides
shared utilities for config access and filtering.
"""

from abc import ABC, abstractmethod
from typing import List
from src.api.schemas import RawJobListing


class BaseScrapeStrategy(ABC):
    """Abstract base class for all job board scraping strategies."""

    def __init__(self, config: dict):
        self.config = config
        self.source_name = config.get("source", "unknown")

    @abstractmethod
    async def fetch(self) -> List[RawJobListing]:
        """Fetch raw job listings from the source.

        Each concrete strategy implements this with source-specific
        API calls and response parsing.
        """
        ...

    def get_field_map(self) -> dict:
        """Get the field mapping from source fields to our schema."""
        return self.config.get("field_map", {})

    def get_rate_limit(self) -> float:
        """Get the rate limit delay in seconds."""
        return self.config.get("rate_limit_seconds", 2.0)

    def get_headers(self) -> dict:
        """Get request headers from config."""
        return self.config.get("request_headers", {})
