"""Abstract base strategy for job board scrapers.

Mirrors Kivaro's strategy pattern: each job board gets its own concrete
strategy class that implements the fetch() method. The base class provides
shared utilities for config access and filtering.
"""

import asyncio
import httpx
import logging
from abc import ABC, abstractmethod
from typing import List
from src.api.schemas import RawJobListing

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_BASE_DELAY = 2.0  # seconds; doubles each attempt (2 → 4 → 8)


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

    # ------------------------------------------------------------------
    # Shared HTTP helpers with exponential-backoff retry
    # ------------------------------------------------------------------

    async def _http_get_json(
        self, client: httpx.AsyncClient, url: str, **kwargs
    ):
        """GET a URL and return parsed JSON.

        Retries up to _MAX_ATTEMPTS times on transient errors:
        - HTTP 5xx responses
        - Connection / timeout errors
        - Invalid / empty JSON body
        """
        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = await client.get(url, **kwargs)
                if response.status_code >= 500:
                    # Treat 5xx as a retryable exception so the except
                    # branch below handles backoff uniformly.
                    raise httpx.HTTPStatusError(
                        f"HTTP {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as e:
                retryable = (
                    isinstance(e, httpx.RequestError)
                    or isinstance(e, ValueError)  # JSON decode error
                    or (
                        isinstance(e, httpx.HTTPStatusError)
                        and e.response.status_code >= 500
                    )
                )
                if retryable and attempt < _MAX_ATTEMPTS - 1:
                    delay = _BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        "[%s] Transient error, retrying in %.0fs (attempt %d/%d): %s",
                        url, delay, attempt + 1, _MAX_ATTEMPTS, e,
                    )
                    await asyncio.sleep(delay)
                else:
                    raise

    async def _http_get_text(
        self, client: httpx.AsyncClient, url: str, **kwargs
    ) -> str:
        """GET a URL and return the response body as text.

        Retries up to _MAX_ATTEMPTS times on transient errors:
        - HTTP 5xx responses
        - Connection / timeout errors
        """
        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = await client.get(url, **kwargs)
                if response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"HTTP {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response.text
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                retryable = (
                    isinstance(e, httpx.RequestError)
                    or (
                        isinstance(e, httpx.HTTPStatusError)
                        and e.response.status_code >= 500
                    )
                )
                if retryable and attempt < _MAX_ATTEMPTS - 1:
                    delay = _BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        "[%s] Transient error, retrying in %.0fs (attempt %d/%d): %s",
                        url, delay, attempt + 1, _MAX_ATTEMPTS, e,
                    )
                    await asyncio.sleep(delay)
                else:
                    raise

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------

    def get_field_map(self) -> dict:
        """Get the field mapping from source fields to our schema."""
        return self.config.get("field_map", {})

    def get_rate_limit(self) -> float:
        """Get the rate limit delay in seconds."""
        return self.config.get("rate_limit_seconds", 2.0)

    def get_headers(self) -> dict:
        """Get request headers from config."""
        return self.config.get("request_headers", {})
