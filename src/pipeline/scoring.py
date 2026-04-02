"""Attractiveness scoring for job listings.

Computes a 0-100 composite score used as the default sort order.
Higher scores = more attractive listings shown first.
"""

import logging
from datetime import datetime, timezone

from src.pipeline.filter import relevance_score

logger = logging.getLogger(__name__)

# Source reliability tiers
SOURCE_SCORES = {
    "adzuna": 15,
    "remoteok": 14,
    "weworkremotely": 13,
    "arbeitnow": 12,
    "jobicy": 11,
    "hnfreelance": 7,
    "forhire": 5,
}

# Companies that are placeholders, not real employer names
PLACEHOLDER_COMPANIES = {"reddit r/forhire", "hacker news", "", "unknown"}

# Recency brackets (days -> points out of 15)
RECENCY_BRACKETS = [
    (3, 15),
    (7, 12),
    (14, 9),
    (30, 6),
    (60, 3),
]


def compute_attractiveness_score(row: dict) -> int:
    """Compute attractiveness score (0-100) for a job listing.

    Components:
    - Relevance tier:     20 pts (core SWE=20, adjacent=10)
    - Salary data:        15 pts (real=15, estimated high-conf=8, medium=4)
    - Quality score:      15 pts (field completeness)
    - Source reliability:  15 pts
    - Real company name:  10 pts
    - Recency:            15 pts
    - HN engagement:      10 pts bonus (only for HN listings with good scores)
    """
    score = 0

    # 1. Relevance tier (max 20)
    title = row.get("title") or ""
    tags = row.get("tags") or []
    tier = relevance_score(title, tags)
    score += tier * 10  # tier 2 = 20, tier 1 = 10

    # 2. Salary data (max 15, mutually exclusive)
    has_salary = row.get("salary_min") or row.get("salary_max")
    is_estimated = row.get("salary_estimated", False)
    if has_salary and not is_estimated:
        score += 15
    elif has_salary and is_estimated:
        confidence = row.get("salary_confidence") or 0
        if confidence >= 0.6:
            score += 8
        elif confidence >= 0.4:
            score += 4

    # 3. Quality score (max 15)
    quality = row.get("quality_score") or 0
    score += round(quality / 100 * 15)

    # 4. Source reliability (max 15)
    source = row.get("source") or ""
    score += SOURCE_SCORES.get(source, 5)

    # 5. Real company name (max 10)
    company = (row.get("company") or "").lower().strip()
    if company and company not in PLACEHOLDER_COMPANIES:
        score += 10

    # 6. Recency (max 15)
    posted = row.get("posted_at") or row.get("first_seen")
    if posted:
        try:
            if isinstance(posted, str):
                # Handle various date formats
                for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                            "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                            "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"):
                    try:
                        posted_dt = datetime.strptime(posted, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    posted_dt = None
            else:
                posted_dt = posted

            if posted_dt:
                if posted_dt.tzinfo is None:
                    posted_dt = posted_dt.replace(tzinfo=timezone.utc)
                age_days = (datetime.now(timezone.utc) - posted_dt).days
                for max_days, points in RECENCY_BRACKETS:
                    if age_days <= max_days:
                        score += points
                        break
                # older than 60 days = 0 points
        except Exception:
            pass  # Can't parse date, no recency bonus

    # 7. HN engagement bonus (up to 10 for HN listings)
    if source == "hnfreelance":
        hn_score = row.get("hn_score") or 0
        hn_comments = row.get("hn_comments") or 0
        if hn_score >= 5 or hn_comments >= 3:
            score += 10
        elif hn_score >= 3 or hn_comments >= 1:
            score += 5

    return min(score, 100)
