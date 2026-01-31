"""Humanization layer for API requests.

Mirrors Kivaro's anti-bot evasion patterns: jittered delays between
requests, rate limiting, and randomized behavior to appear as organic
traffic rather than automated scraping.
"""

import asyncio
import random
import logging

logger = logging.getLogger(__name__)


class Humanizer:
    """Adds human-like delay patterns to scraping operations."""

    def __init__(self, base_delay: float = 1.5, delay_multiplier: float = 1.0):
        self.base_delay = base_delay
        self.delay_multiplier = delay_multiplier
        self._request_count = 0

    async def delay(self):
        """Wait a randomized duration before the next request.

        Uses jittered delays that increase slightly over consecutive requests,
        simulating a real user browsing through pages.
        """
        self._request_count += 1

        # Base jitter: 0.5x to 1.5x the base delay
        jitter = random.uniform(0.5, 1.5)
        wait = self.base_delay * jitter * self.delay_multiplier

        # Every 5th request, add a longer "reading" pause (like Kivaro's idle_stare)
        if self._request_count % 5 == 0:
            wait += random.uniform(1.0, 3.0)
            logger.debug("Idle stare pause: %.1fs", wait)

        logger.debug("Humanized delay: %.2fs (request #%d)", wait, self._request_count)
        await asyncio.sleep(wait)

    def escalate(self, multiplier: float):
        """Increase delays for retry attempts (mirrors Kivaro's 3x/5x escalation)."""
        return Humanizer(
            base_delay=self.base_delay,
            delay_multiplier=self.delay_multiplier * multiplier,
        )

    def reset(self):
        """Reset request counter for a new scrape session."""
        self._request_count = 0
