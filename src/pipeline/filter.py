"""Relevance filter and scoring for software / tech job listings.

is_relevant()      — binary keep/drop used during scrape ingestion
relevance_score()  — 0/1/2 tier used to sort the /api/jobs feed:
                       2 = core SWE (writes code)
                       1 = adjacent tech (PM, UX, QA, …)
                       0 = borderline (shouldn't reach the DB after filtering)
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

# ------------------------------------------------------------------
# Tier 2 — core SWE: roles that primarily involve writing code
# ------------------------------------------------------------------
CORE_SWE_KEYWORDS = [
    "software engineer", "software developer",
    "backend", "frontend", "front-end", "full-stack", "fullstack",
    "developer", "programmer",
    "engineer",          # catches most engineering roles
    "devops", "devsecops", "sre",
    "data engineer", "data scientist", "ml engineer", "ai engineer",
    "machine learning", "deep learning",
    "firmware", "embedded",
    "cloud engineer", "infrastructure engineer", "platform engineer",
    "security engineer", "cybersecurity",
]

# Tier 1 — adjacent tech: technical but not primarily coding
ADJACENT_TECH_KEYWORDS = [
    "product manager", "product owner",
    "ux designer", "ui designer", "product designer",
    "qa", "quality engineer", "test engineer", "sdet",
    "engineering manager", "tech lead", "technical lead",
    "architect",
    "data analyst",
    "ai trainer", "coding",
]

# Precompile patterns once at import time
_TITLE_PATTERNS = [
    re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
    for kw in TECH_TITLE_KEYWORDS
]
_CORE_SWE_PATTERNS = [
    re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
    for kw in CORE_SWE_KEYWORDS
]
_ADJACENT_PATTERNS = [
    re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
    for kw in ADJACENT_TECH_KEYWORDS
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


def relevance_score(title: str, tags: list) -> int:
    """Return a sort-priority score for a job listing.

    2 = core SWE (shown first)
    1 = adjacent tech
    0 = borderline / unclassified
    """
    if any(p.search(title) for p in _CORE_SWE_PATTERNS):
        return 2
    if any(p.search(title) for p in _ADJACENT_PATTERNS):
        return 1
    # Tag-based fallback
    tags_lower = {t.lower() for t in (tags or [])}
    if tags_lower & {"software", "engineering", "developer", "backend", "frontend", "devops"}:
        return 2
    if tags_lower & TECH_TAG_KEYWORDS:
        return 1
    return 0
