"""Stability tracker to prevent false removal signals.

Mirrors Kivaro's game_stability_tracker: a listing is only confirmed as
removed after it has been missing for N consecutive scrape runs. This
prevents transient API failures or pagination issues from triggering
false "removed" alerts.
"""

from typing import Set, Dict
from supabase import Client

REMOVAL_THRESHOLD = 3  # Consecutive misses before confirming removal


def update_stability(
    db: Client,
    source: str,
    current_ids: Set[str],
) -> Dict[str, set]:
    """Update consecutive_misses for all listings of a source.

    Returns dict with 'confirmed_removals' and 'tentative_removals' sets.
    """
    # Get all active listings for this source
    result = db.table("job_listings").select(
        "external_id, consecutive_misses"
    ).eq("source", source).eq("is_active", True).execute()

    confirmed_removals = set()
    tentative_removals = set()

    for row in result.data:
        ext_id = row["external_id"]
        misses = row["consecutive_misses"]

        if ext_id in current_ids:
            # Found — reset consecutive misses
            db.table("job_listings").update({
                "consecutive_misses": 0,
                "last_seen": "now()",
            }).eq("external_id", ext_id).eq("source", source).execute()
        else:
            # Missing — increment consecutive misses
            new_misses = misses + 1
            if new_misses >= REMOVAL_THRESHOLD:
                confirmed_removals.add(ext_id)
                db.table("job_listings").update({
                    "consecutive_misses": new_misses,
                    "is_active": False,
                }).eq("external_id", ext_id).eq("source", source).execute()
            else:
                tentative_removals.add(ext_id)
                db.table("job_listings").update({
                    "consecutive_misses": new_misses,
                }).eq("external_id", ext_id).eq("source", source).execute()

    return {
        "confirmed_removals": confirmed_removals,
        "tentative_removals": tentative_removals,
    }
