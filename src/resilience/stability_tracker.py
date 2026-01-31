"""Stability tracker to prevent false removal signals.

Mirrors Kivaro's game_stability_tracker: a listing is only confirmed as
removed after it has been missing for N consecutive scrape runs. This
prevents transient API failures or pagination issues from triggering
false "removed" alerts.
"""

from typing import Set, Dict
import aiosqlite

REMOVAL_THRESHOLD = 3  # Consecutive misses before confirming removal


async def update_stability(
    db: aiosqlite.Connection,
    source: str,
    current_ids: Set[str],
) -> Dict[str, set]:
    """Update consecutive_misses for all listings of a source.

    Returns dict with 'confirmed_removals' and 'tentative_removals' sets.
    """
    # Get all active listings for this source
    cursor = await db.execute(
        "SELECT external_id, consecutive_misses FROM job_listings WHERE source = ? AND is_active = 1",
        (source,),
    )
    rows = await cursor.fetchall()

    confirmed_removals = set()
    tentative_removals = set()

    for row in rows:
        ext_id = row[0]
        misses = row[1]

        if ext_id in current_ids:
            # Found — reset consecutive misses
            await db.execute(
                "UPDATE job_listings SET consecutive_misses = 0, last_seen = datetime('now') WHERE external_id = ? AND source = ?",
                (ext_id, source),
            )
        else:
            # Missing — increment consecutive misses
            new_misses = misses + 1
            if new_misses >= REMOVAL_THRESHOLD:
                confirmed_removals.add(ext_id)
                await db.execute(
                    "UPDATE job_listings SET consecutive_misses = ?, is_active = 0 WHERE external_id = ? AND source = ?",
                    (new_misses, ext_id, source),
                )
            else:
                tentative_removals.add(ext_id)
                await db.execute(
                    "UPDATE job_listings SET consecutive_misses = ? WHERE external_id = ? AND source = ?",
                    (new_misses, ext_id, source),
                )

    return {
        "confirmed_removals": confirmed_removals,
        "tentative_removals": tentative_removals,
    }
