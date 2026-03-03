"""Stability tracker to prevent false removal signals.

Mirrors Kivaro's game_stability_tracker: a listing is only confirmed as
removed after it has been missing for N consecutive scrape runs. This
prevents transient API failures or pagination issues from triggering
false "removed" alerts.
"""

import time
import logging
from typing import Set, Dict
from supabase import Client

logger = logging.getLogger(__name__)

REMOVAL_THRESHOLD = 3  # Consecutive misses before confirming removal


def _db_execute(query, max_attempts: int = 3, base_delay: float = 2.0):
    """Execute a Supabase query with exponential backoff on transient 5xx errors."""
    from postgrest.exceptions import APIError

    for attempt in range(max_attempts):
        try:
            return query.execute()
        except APIError as e:
            code = str(e.args[0].get("code", "")) if e.args else ""
            is_transient = code.startswith("5") or code in ("502", "503", "504")
            if is_transient and attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "Supabase transient error (code %s), retrying in %.0fs (attempt %d/%d)",
                    code, delay, attempt + 1, max_attempts,
                )
                time.sleep(delay)
            else:
                raise


def update_stability(
    db: Client,
    source: str,
    current_ids: Set[str],
) -> Dict[str, set]:
    """Update consecutive_misses for all listings of a source.

    Returns dict with 'confirmed_removals' and 'tentative_removals' sets.
    """
    # Get all active listings for this source
    result = _db_execute(
        db.table("job_listings").select(
            "external_id, consecutive_misses"
        ).eq("source", source).eq("is_active", True)
    )

    confirmed_removals = set()
    tentative_removals = set()

    for row in result.data:
        ext_id = row["external_id"]
        misses = row["consecutive_misses"]

        if ext_id in current_ids:
            # Found — reset consecutive misses
            _db_execute(
                db.table("job_listings").update({
                    "consecutive_misses": 0,
                    "last_seen": "now()",
                }).eq("external_id", ext_id).eq("source", source)
            )
        else:
            # Missing — increment consecutive misses
            new_misses = misses + 1
            if new_misses >= REMOVAL_THRESHOLD:
                confirmed_removals.add(ext_id)
                _db_execute(
                    db.table("job_listings").update({
                        "consecutive_misses": new_misses,
                        "is_active": False,
                    }).eq("external_id", ext_id).eq("source", source)
                )
            else:
                tentative_removals.add(ext_id)
                _db_execute(
                    db.table("job_listings").update({
                        "consecutive_misses": new_misses,
                    }).eq("external_id", ext_id).eq("source", source)
                )

    return {
        "confirmed_removals": confirmed_removals,
        "tentative_removals": tentative_removals,
    }
