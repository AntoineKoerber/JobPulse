"""AI-calibrated salary estimation for jobs without salary data.

Two-stage approach:
1. Statistical model — aggregates existing salary data by role×region×seniority
2. AI fallback — GPT-4o-mini validates low-confidence estimates and handles unknowns

The statistical model improves over time as more real salary data is scraped,
reducing AI calls automatically.
"""

import logging
import os
import re
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from statistics import median

logger = logging.getLogger(__name__)

# Exchange rates for normalizing to USD (same as frontend)
EXCHANGE_RATES = {"USD": 1.0, "EUR": 0.92, "GBP": 0.79, "CHF": 0.88}

# ── Title Categorization ────────────────────────────────────────────

TITLE_BUCKETS = {
    "software_engineer": [
        "software engineer", "software developer", "software dev",
    ],
    "frontend": [
        "frontend", "front-end", "front end", "ui developer", "ui engineer",
        "react developer", "angular developer", "vue developer",
    ],
    "backend": [
        "backend", "back-end", "back end", "server-side",
    ],
    "fullstack": [
        "full-stack", "fullstack", "full stack",
    ],
    "devops": [
        "devops", "devsecops", "site reliability", "sre", "infrastructure engineer",
        "platform engineer", "cloud engineer",
    ],
    "data_scientist": [
        "data scientist", "machine learning", "ml engineer", "ai engineer",
        "deep learning", "nlp engineer",
    ],
    "data_engineer": [
        "data engineer", "data platform", "analytics engineer", "etl",
    ],
    "data_analyst": [
        "data analyst", "business analyst", "analytics",
    ],
    "security": [
        "security engineer", "cybersecurity", "appsec", "infosec",
        "penetration tester", "security analyst",
    ],
    "mobile": [
        "mobile developer", "ios developer", "android developer",
        "react native", "flutter developer", "mobile engineer",
    ],
    "qa": [
        "qa engineer", "test engineer", "sdet", "quality engineer",
        "quality assurance",
    ],
    "product_manager": [
        "product manager", "product owner", "program manager",
    ],
    "designer": [
        "ux designer", "ui designer", "product designer", "ux researcher",
    ],
    "engineering_manager": [
        "engineering manager", "tech lead", "technical lead", "head of engineering",
        "vp of engineering", "vp engineering", "director of engineering",
        "cto",
    ],
    "architect": [
        "solutions architect", "cloud architect", "software architect",
        "enterprise architect", "system architect",
    ],
}

# Compile patterns once
_BUCKET_PATTERNS = {
    bucket: [re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE) for kw in keywords]
    for bucket, keywords in TITLE_BUCKETS.items()
}

SENIORITY_PATTERN = re.compile(
    r"\b(junior|jr\.?|senior|sr\.?|mid[- ]?level|entry[- ]?level|lead|principal|staff)\b",
    re.IGNORECASE,
)

REGION_MAP = {
    "us": [
        "usa", "united states", "us", "america", "new york", "san francisco",
        "los angeles", "chicago", "seattle", "austin", "boston", "denver",
        "miami", "atlanta", "dallas", "portland", "washington",
    ],
    "europe": [
        "germany", "uk", "united kingdom", "france", "netherlands", "spain",
        "portugal", "italy", "sweden", "norway", "denmark", "finland",
        "switzerland", "austria", "ireland", "belgium", "poland", "czech",
        "romania", "hungary", "berlin", "london", "paris", "amsterdam",
        "munich", "dublin", "stockholm", "eu", "emea",
    ],
    "asia": [
        "india", "japan", "singapore", "south korea", "hong kong", "china",
        "taiwan", "vietnam", "thailand", "philippines", "indonesia",
        "bangalore", "mumbai", "tokyo", "apac",
    ],
    "remote_global": [
        "remote", "worldwide", "global", "anywhere", "distributed",
    ],
}


@dataclass
class SalaryEstimate:
    salary_min: int
    salary_max: int
    currency: str
    confidence: float
    method: str  # "statistical", "ai", "hybrid"


def categorize_title(title: str) -> str:
    """Map a job title to a bucket like 'software_engineer', 'frontend', etc."""
    lower = title.lower()
    for bucket, patterns in _BUCKET_PATTERNS.items():
        if any(p.search(lower) for p in patterns):
            return bucket
    return "other"


def extract_seniority(title: str) -> str:
    """Extract seniority from title."""
    m = SENIORITY_PATTERN.search(title)
    if not m:
        return "mid"
    token = m.group(1).lower().rstrip(".")
    if token in ("junior", "jr"):
        return "junior"
    if token in ("senior", "sr"):
        return "senior"
    if token in ("lead", "principal", "staff"):
        return "senior"
    if "entry" in token:
        return "junior"
    return "mid"


def detect_region(location: Optional[str]) -> str:
    """Map a location string to a region bucket."""
    if not location:
        return "remote_global"
    lower = location.lower()
    for region, keywords in REGION_MAP.items():
        for kw in keywords:
            if kw in lower:
                return region
    return "unknown"


def to_usd(amount: int, currency: str) -> int:
    """Convert a salary amount to USD."""
    rate = EXCHANGE_RATES.get(currency, 1.0)
    return int(amount / rate)


def from_usd(amount: int, currency: str) -> int:
    """Convert USD back to a target currency."""
    rate = EXCHANGE_RATES.get(currency, 1.0)
    return int(amount * rate)


# ── Statistical Estimator ───────────────────────────────────────────

@dataclass
class _BucketStats:
    min_values: list = field(default_factory=list)
    max_values: list = field(default_factory=list)
    count: int = 0


class StatisticalEstimator:
    """Estimates salaries from aggregated existing data."""

    def __init__(self):
        self._cache: dict[tuple[str, str, str], _BucketStats] = {}

    def build_model(self, db) -> int:
        """Build salary lookup from all active listings with salary data.

        Returns the total number of salary data points used.
        """
        self._cache.clear()

        all_salary_rows = []
        offset = 0
        while True:
            batch = db.table("job_listings").select(
                "title, location, salary_min, salary_max, currency"
            ).eq("is_active", True).not_.is_(
                "salary_min", "null"
            ).or_(
                "salary_estimated.is.null,salary_estimated.eq.false"
            ).order("id").range(offset, offset + 999).execute()
            all_salary_rows.extend(batch.data)
            if len(batch.data) < 1000:
                break
            offset += 1000

        total = 0
        for row in all_salary_rows:
            sal_min = row.get("salary_min")
            sal_max = row.get("salary_max")
            currency = row.get("currency") or "USD"

            if not sal_min and not sal_max:
                continue
            # Skip zero/very low values — bad data
            if (sal_min and sal_min < 5000) or (sal_max and sal_max < 5000):
                continue

            title = row.get("title") or ""
            location = row.get("location") or ""

            bucket = categorize_title(title)
            seniority = extract_seniority(title)
            region = detect_region(location)

            key = (bucket, seniority, region)
            if key not in self._cache:
                self._cache[key] = _BucketStats()

            stats = self._cache[key]
            if sal_min:
                stats.min_values.append(to_usd(sal_min, currency))
            if sal_max:
                stats.max_values.append(to_usd(sal_max or sal_min, currency))
            stats.count += 1
            total += 1

        logger.info(
            "Statistical model built: %d data points across %d categories",
            total, len(self._cache),
        )
        return total

    def estimate(self, title: str, location: Optional[str]) -> Optional[SalaryEstimate]:
        """Estimate salary for a single job. Returns None if no data available."""
        bucket = categorize_title(title)
        seniority = extract_seniority(title)
        region = detect_region(location)

        # Try exact match, then relax
        for key, confidence_penalty in [
            ((bucket, seniority, region), 0.0),
            ((bucket, "mid", region), 0.1),
            ((bucket, seniority, "remote_global"), 0.15),
            ((bucket, "mid", "remote_global"), 0.25),
        ]:
            stats = self._cache.get(key)
            if stats and stats.count >= 3:
                count = stats.count
                if count >= 20:
                    base_conf = 0.85
                elif count >= 10:
                    base_conf = 0.65
                elif count >= 5:
                    base_conf = 0.5
                else:
                    base_conf = 0.35

                confidence = max(0.1, base_conf - confidence_penalty)

                # Determine output currency based on region
                out_currency = "USD"
                if region == "europe":
                    out_currency = "EUR"
                elif region == "asia":
                    out_currency = "USD"  # Most Asia jobs listed in USD

                min_val = int(median(stats.min_values)) if stats.min_values else 0
                max_val = int(median(stats.max_values)) if stats.max_values else 0

                if min_val <= 0 or max_val <= 0:
                    continue  # Skip this match, try relaxed

                return SalaryEstimate(
                    salary_min=from_usd(min_val, out_currency),
                    salary_max=from_usd(max_val, out_currency),
                    currency=out_currency,
                    confidence=round(confidence, 2),
                    method="statistical",
                )

        return None


# ── AI Estimator ────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a salary estimation expert for tech jobs. Given job details, estimate realistic annual salary ranges.

Rules:
- Return a JSON array with one object per job in the same order
- Each object: {"salary_min": int, "salary_max": int, "currency": "USD"|"EUR"|"GBP", "confidence": 0.0-1.0}
- Use USD unless the location clearly indicates EUR (Europe) or GBP (UK)
- Base estimates on current market rates for the role, seniority, and region
- confidence should reflect how certain you are (0.4-0.9 range)
- If a statistical_hint is provided, validate and adjust it rather than estimating from scratch
- Return ONLY the JSON array, no other text"""


class AIEstimator:
    """Calls GPT-4o-mini for salary estimation."""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.total_tokens = 0

    def estimate_batch(
        self,
        jobs: list[dict],
        statistical_hints: Optional[list[Optional[SalaryEstimate]]] = None,
    ) -> list[Optional[SalaryEstimate]]:
        """Estimate salaries for a batch of up to 20 jobs."""
        if not jobs:
            return []

        # Build job descriptions for the prompt
        items = []
        for i, job in enumerate(jobs):
            item = {
                "title": job.get("title", ""),
                "company": job.get("company", ""),
                "location": job.get("location", ""),
                "tags": job.get("tags", []),
            }
            if statistical_hints and statistical_hints[i]:
                hint = statistical_hints[i]
                item["statistical_hint"] = {
                    "salary_min": hint.salary_min,
                    "salary_max": hint.salary_max,
                    "currency": hint.currency,
                    "confidence": hint.confidence,
                }
            items.append(item)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(items)},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
            )

            if response.usage:
                self.total_tokens += response.usage.total_tokens

            content = response.choices[0].message.content
            logger.info("AI response: %s", content[:500])
            parsed = json.loads(content)

            # json_object mode always returns a dict — find the array inside
            if isinstance(parsed, dict):
                estimates_raw = None
                for val in parsed.values():
                    if isinstance(val, list):
                        estimates_raw = val
                        break
                if estimates_raw is None:
                    # Single estimate wrapped in a dict
                    if "salary_min" in parsed:
                        estimates_raw = [parsed]
                    else:
                        logger.warning("Unexpected AI response structure: %s", list(parsed.keys()))
                        estimates_raw = []
            else:
                estimates_raw = parsed

            results = []
            for est in estimates_raw:
                try:
                    results.append(SalaryEstimate(
                        salary_min=int(est["salary_min"]),
                        salary_max=int(est["salary_max"]),
                        currency=est.get("currency", "USD"),
                        confidence=float(est.get("confidence", 0.5)),
                        method="ai",
                    ))
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning("Failed to parse AI estimate: %s", e)
                    results.append(None)

            # Pad if AI returned fewer results than expected
            while len(results) < len(jobs):
                results.append(None)

            return results[:len(jobs)]

        except Exception as e:
            logger.error("AI estimation failed: %s", e)
            return [None] * len(jobs)


# ── Coordinator ─────────────────────────────────────────────────────

MIN_CONFIDENCE = 0.4
AI_BATCH_SIZE = 20


async def estimate_salaries(db, openai_api_key: Optional[str] = None) -> dict:
    """Main entry point: estimate salaries for all jobs missing salary data.

    Returns summary stats.
    """
    # 0. Clean up bad estimates — reset values below $5000 (matches _is_valid_estimate)
    reset_fields = {
        "salary_min": None,
        "salary_max": None,
        "currency": None,
        "salary_estimated": False,
        "salary_confidence": None,
        "salary_estimation_method": None,
        "salary_estimated_at": None,
    }
    cleanup = db.table("job_listings").update(
        reset_fields
    ).eq("salary_estimated", True).lt("salary_min", 5000).execute()
    cleaned = len(cleanup.data) if cleanup.data else 0

    cleanup2 = db.table("job_listings").update(
        reset_fields
    ).eq("salary_estimated", True).lt("salary_max", 5000).execute()
    cleaned += len(cleanup2.data) if cleanup2.data else 0

    # Also clean up bad raw-extracted salaries (not estimated, but garbage values)
    cleanup3 = db.table("job_listings").update({
        "salary_min": None,
        "salary_max": None,
        "currency": None,
    }).or_(
        "salary_estimated.is.null,salary_estimated.eq.false"
    ).lt("salary_min", 100).not_.is_("salary_min", "null").execute()
    cleaned += len(cleanup3.data) if cleanup3.data else 0

    if cleaned:
        logger.info("Cleaned %d bad salary values", cleaned)

    # 1. Build statistical model
    stat = StatisticalEstimator()
    total_samples = stat.build_model(db)

    # 2. Find jobs needing estimation (paginated)
    # a) Jobs with no salary at all
    candidates = []
    offset = 0
    while True:
        batch = db.table("job_listings").select(
            "id, title, company, location, tags, salary_min, salary_max, "
            "salary_estimated, salary_estimated_at"
        ).eq("is_active", True).is_("salary_min", "null").is_(
            "salary_max", "null"
        ).order("id").range(offset, offset + 999).execute()
        candidates.extend(batch.data)
        if len(batch.data) < 1000:
            break
        offset += 1000

    # b) Jobs with stale estimates (older than 14 days) — re-estimate
    stale_cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    stale_candidates = []
    offset = 0
    while True:
        batch = db.table("job_listings").select(
            "id, title, company, location, tags, salary_min, salary_max, "
            "salary_estimated, salary_estimated_at"
        ).eq("is_active", True).eq(
            "salary_estimated", True
        ).lt(
            "salary_estimated_at", stale_cutoff
        ).order("id").range(offset, offset + 999).execute()
        stale_candidates.extend(batch.data)
        if len(batch.data) < 1000:
            break
        offset += 1000

    if stale_candidates:
        logger.info("Found %d stale estimates (>14 days) to re-estimate", len(stale_candidates))
        # Bulk reset stale estimates
        db.table("job_listings").update({
            "salary_min": None,
            "salary_max": None,
            "currency": None,
            "salary_estimated": False,
            "salary_confidence": None,
            "salary_estimation_method": None,
            "salary_estimated_at": None,
        }).eq("is_active", True).eq(
            "salary_estimated", True
        ).lt("salary_estimated_at", stale_cutoff).execute()
        candidates.extend(stale_candidates)

    if not candidates:
        logger.info("No jobs need salary estimation")
        return {"total_missing": 0, "statistical": 0, "ai": 0, "skipped": 0}

    logger.info("Found %d jobs needing salary estimation", len(candidates))

    # 3. Run statistical estimation on all candidates
    stat_count = 0
    ai_queue = []  # (row, stat_hint)

    for row in candidates:
        title = row.get("title") or ""
        location = row.get("location")

        est = stat.estimate(title, location)

        if est and est.confidence >= MIN_CONFIDENCE:
            # Good enough — use statistical estimate directly
            if _save_estimate(db, row["id"], est):
                stat_count += 1
            else:
                ai_queue.append((row, None))
                continue
        else:
            # Queue for AI (with statistical hint if available)
            ai_queue.append((row, est))

    logger.info(
        "Statistical: %d estimated, %d queued for AI",
        stat_count, len(ai_queue),
    )

    # 4. AI estimation (if key available)
    ai_count = 0
    skipped = 0

    if ai_queue and openai_api_key:
        ai = AIEstimator(api_key=openai_api_key)

        # Process in batches
        for batch_start in range(0, len(ai_queue), AI_BATCH_SIZE):
            batch = ai_queue[batch_start:batch_start + AI_BATCH_SIZE]
            jobs = [
                {
                    "title": r.get("title", ""),
                    "company": r.get("company", ""),
                    "location": r.get("location", ""),
                    "tags": r.get("tags", []),
                }
                for r, _ in batch
            ]
            hints = [hint for _, hint in batch]

            ai_results = ai.estimate_batch(jobs, statistical_hints=hints)

            for (row, stat_hint), ai_est in zip(batch, ai_results):
                if ai_est:
                    # If we had a statistical hint, mark as hybrid
                    if stat_hint:
                        ai_est.method = "hybrid"
                    _save_estimate(db, row["id"], ai_est)
                    ai_count += 1
                elif stat_hint:
                    # AI failed but we have a low-confidence stat estimate
                    _save_estimate(db, row["id"], stat_hint)
                    stat_count += 1
                else:
                    skipped += 1

        logger.info("AI estimation used %d tokens", ai.total_tokens)
    elif ai_queue:
        # No API key — use low-confidence statistical estimates where available
        for row, stat_hint in ai_queue:
            if stat_hint:
                _save_estimate(db, row["id"], stat_hint)
                stat_count += 1
            else:
                skipped += 1
        logger.info("No OPENAI_API_KEY — skipped %d AI estimations", skipped)

    summary = {
        "total_missing": len(candidates),
        "statistical": stat_count,
        "ai": ai_count,
        "skipped": skipped,
        "model_data_points": total_samples,
    }
    logger.info("Salary estimation complete: %s", summary)
    return summary


def _is_valid_estimate(est: SalaryEstimate) -> bool:
    """Check that an estimate has sensible values."""
    if est.salary_min <= 0 or est.salary_max <= 0:
        return False
    if est.salary_min < 5000 or est.salary_max < 5000:
        return False
    if est.salary_max > 2000000:
        return False
    return True


def _save_estimate(db, listing_id: int, est: SalaryEstimate):
    """Persist a salary estimate to the database."""
    if not _is_valid_estimate(est):
        logger.warning("Skipping invalid estimate for listing %d: min=%d max=%d",
                       listing_id, est.salary_min, est.salary_max)
        return False
    now = datetime.now(timezone.utc).isoformat()
    db.table("job_listings").update({
        "salary_min": est.salary_min,
        "salary_max": est.salary_max,
        "currency": est.currency,
        "salary_estimated": True,
        "salary_confidence": est.confidence,
        "salary_estimation_method": est.method,
        "salary_estimated_at": now,
    }).eq("id", listing_id).execute()
    return True
