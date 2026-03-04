"""Relevance filter: keep only software / tech-adjacent job listings.

A listing is considered relevant if its normalised title contains at least
one keyword from TECH_TITLE_KEYWORDS.  Tags are used as a fallback when
the title alone does not match.
"""

import re
from typing import List
from src.api.schemas import JobListing

# Checked as word-boundary substrings against the lowercased title.
# Order doesn't matter — all are tested.
TECH_TITLE_KEYWORDS = [
    # Core engineering
    "engineer",         # engineer / engineering / engineers
    "developer",        # developer / development
    "programmer",
    "architect",        # solutions architect, cloud architect, …
    "devops",
    "devsecops",
    "reliability",      # site reliability engineer / SRE
    # Specialisations
    "backend",
    "frontend",
    "front-end",
    "full-stack",
    "fullstack",
    "software",
    "firmware",
    "embedded",
    "infrastructure",
    "platform",
    # Data / AI / ML
    "data scientist",
    "data engineer",
    "data analyst",
    "machine learning",
    "deep learning",
    "artificial intelligence",
    "ai trainer",
    "ai engineer",
    "ml engineer",
    "llm",
    # Security
    "cybersecurity",
    "cyber security",
    "appsec",
    "infosec",
    # Other disciplines
    "blockchain",
    "coding",
    # Product / design (technical)
    "product manager",
    "product owner",
    "ux designer",
    "ui designer",
    "product designer",
    # QA / testing
    "qa engineer",
    "test engineer",
    "sdet",
    "quality engineer",
    # Leadership
    "engineering manager",
    "tech lead",
    "technical lead",
    "cto",
    "vp of engineering",
    "vp engineering",
    "head of engineering",
]

# Tech tags that act as a fallback when the title alone doesn't match.
TECH_TAG_KEYWORDS = {
    "software", "engineering", "developer", "backend", "frontend",
    "devops", "cloud", "data", "ml", "ai", "security", "mobile",
    "ios", "android", "python", "javascript", "typescript", "rust",
    "golang", "java", "kotlin", "swift", "react", "node",
}

# Precompile patterns once at import time
_TITLE_PATTERNS = [
    re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
    for kw in TECH_TITLE_KEYWORDS
]


def is_relevant(listing: JobListing) -> bool:
    """Return True if the listing is tech/software-relevant."""
    title = listing.title or ""
    if any(p.search(title) for p in _TITLE_PATTERNS):
        return True

    # Fallback: check tags
    tags = {t.lower() for t in (listing.tags or [])}
    return bool(tags & TECH_TAG_KEYWORDS)


def filter_relevant(listings: List[JobListing]) -> tuple[List[JobListing], int]:
    """Filter a list of listings, returning (relevant, n_dropped)."""
    relevant = [l for l in listings if is_relevant(l)]
    return relevant, len(listings) - len(relevant)
