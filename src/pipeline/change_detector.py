"""Change detection between scrape runs.

Compares current listings against previously known listings for a source,
identifying added, removed, and retained jobs. Works with the stability
tracker to prevent false removal signals from transient failures.
"""

from typing import Set, Dict


def detect_changes(previous_ids: Set[str], current_ids: Set[str]) -> Dict[str, set]:
    """Diff two sets of external IDs to find added, removed, and retained.

    Args:
        previous_ids: External IDs from the last scrape (or currently active in DB).
        current_ids: External IDs from the current scrape.

    Returns:
        Dict with 'added', 'removed', and 'retained' sets.
    """
    return {
        "added": current_ids - previous_ids,
        "removed": previous_ids - current_ids,
        "retained": current_ids & previous_ids,
    }


def build_change_summary(changes: Dict[str, set]) -> Dict[str, int]:
    """Summarize changes into counts for storage in scrape_runs."""
    return {
        "added_count": len(changes["added"]),
        "removed_count": len(changes["removed"]),
        "retained_count": len(changes["retained"]),
        "total_count": len(changes["added"]) + len(changes["retained"]),
    }
