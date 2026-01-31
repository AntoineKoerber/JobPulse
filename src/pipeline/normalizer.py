"""Multi-layer data normalization pipeline for job listings.

Mirrors the Kivaro production scraper's normalization approach:
layered text cleaning for titles, company names, locations, and salaries.
"""

import re
import html
from typing import Optional, Tuple

# Common title abbreviation expansions
TITLE_EXPANSIONS = {
    r"\bSr\.?\s*": "Senior ",
    r"\bJr\.?\s*": "Junior ",
    r"\bEng\.?\b": "Engineer",
    r"\bDev\.?\b": "Developer",
    r"\bMgr\.?\b": "Manager",
    r"\bAdmin\.?\b": "Administrator",
    r"\bOps\.?\b": "Operations",
    r"\bArch\.?\b": "Architect",
    r"\bMkt\.?\b": "Marketing",
    r"\bProd\.?\b": "Product",
}

# Known acronyms to preserve as uppercase
ACRONYMS = {
    "api", "aws", "gcp", "ui", "ux", "qa", "ci", "cd", "ml", "ai",
    "sre", "cto", "ceo", "vp", "hr", "it", "sql", "nosql", "saas",
    "b2b", "b2c", "sdk", "ios", "devops", "devsecops",
}

# Location normalization mappings
REMOTE_VARIANTS = {
    "remote", "anywhere", "worldwide", "global", "work from home",
    "wfh", "distributed", "remote - worldwide",
}


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text)


def normalize_whitespace(text: str) -> str:
    """Collapse whitespace runs and strip."""
    return re.sub(r"\s+", " ", text).strip()


def normalize_title(raw_title: str) -> str:
    """Normalize a job title through multiple cleaning layers."""
    title = strip_html(raw_title)
    title = normalize_whitespace(title)

    # Expand abbreviations
    for pattern, expansion in TITLE_EXPANSIONS.items():
        title = re.sub(pattern, expansion, title, flags=re.IGNORECASE)

    # Title case, preserving acronyms
    words = title.split()
    normalized = []
    for word in words:
        if word.lower() in ACRONYMS:
            normalized.append(word.upper())
        elif word.startswith("(") and word.endswith(")"):
            inner = word[1:-1]
            if inner.lower() in ACRONYMS:
                normalized.append(f"({inner.upper()})")
            else:
                normalized.append(word)
        else:
            normalized.append(word.capitalize() if not any(c.isupper() for c in word[1:]) else word)
    return " ".join(normalized)


def normalize_company(raw_company: str) -> str:
    """Normalize a company name."""
    company = strip_html(raw_company)
    company = normalize_whitespace(company)

    # Remove trailing legal suffixes for consistency
    company = re.sub(r"\s+(Inc\.?|LLC|Ltd\.?|Corp\.?|GmbH|S\.A\.|B\.V\.)$", "", company, flags=re.IGNORECASE)

    return company


def normalize_location(raw_location: Optional[str]) -> Optional[str]:
    """Standardize location strings."""
    if not raw_location:
        return None

    location = strip_html(raw_location)
    location = normalize_whitespace(location)

    if location.lower().strip() in REMOTE_VARIANTS:
        return "Remote"

    # Handle "Remote, US" / "Remote (US)" patterns
    remote_match = re.match(r"remote\s*[,\-/|]\s*(.+)", location, re.IGNORECASE)
    if remote_match:
        region = remote_match.group(1).strip()
        return f"Remote ({region})"

    remote_paren = re.match(r"remote\s*\((.+)\)", location, re.IGNORECASE)
    if remote_paren:
        region = remote_paren.group(1).strip()
        return f"Remote ({region})"

    return location


def extract_salary(
    raw: Optional[str] = None,
    salary_min: Optional[int] = None,
    salary_max: Optional[int] = None,
) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Parse salary information into (min, max, currency).

    Handles both pre-parsed numeric values and raw salary strings like
    "$120k - $180k", "100,000 - 150,000 EUR", etc.
    """
    if salary_min is not None or salary_max is not None:
        return (salary_min, salary_max, "USD")

    if not raw:
        return (None, None, None)

    raw = raw.replace(",", "").strip()

    # Detect currency
    currency = "USD"
    if "EUR" in raw.upper() or "\u20ac" in raw:
        currency = "EUR"
    elif "GBP" in raw.upper() or "\u00a3" in raw:
        currency = "GBP"

    # Find all numbers, handle "k" suffix
    numbers = []
    for match in re.finditer(r"(\d+(?:\.\d+)?)\s*[kK]?", raw):
        val = float(match.group(1))
        if "k" in raw[match.start():match.end()].lower():
            val *= 1000
        numbers.append(int(val))

    if len(numbers) >= 2:
        return (min(numbers), max(numbers), currency)
    elif len(numbers) == 1:
        return (numbers[0], numbers[0], currency)

    return (None, None, None)


def normalize_tags(tags) -> list:
    """Clean and deduplicate tags."""
    if not tags:
        return []
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]

    seen = set()
    cleaned = []
    for tag in tags:
        tag = tag.strip().lower()
        if tag and tag not in seen:
            seen.add(tag)
            cleaned.append(tag)
    return cleaned
