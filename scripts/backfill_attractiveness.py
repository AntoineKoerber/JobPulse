"""One-time backfill: compute attractiveness_score for all existing listings.

Run after applying the add_attractiveness_columns.sql migration:
    python -m scripts.backfill_attractiveness
"""

import logging
import sys

from dotenv import load_dotenv

load_dotenv()

from src.db.database import get_db
from src.pipeline.scoring import compute_attractiveness_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backfill_attractiveness")


def main():
    db = get_db()

    offset = 0
    total = 0
    while True:
        batch = db.table("job_listings").select(
            "id, title, company, tags, quality_score, source, "
            "salary_min, salary_max, salary_estimated, salary_confidence, "
            "posted_at, first_seen"
        ).eq("is_active", True).order("id").range(offset, offset + 999).execute()

        if not batch.data:
            break

        for row in batch.data:
            score = compute_attractiveness_score(row)
            db.table("job_listings").update(
                {"attractiveness_score": score}
            ).eq("id", row["id"]).execute()
            total += 1

        logger.info("Scored %d listings so far (batch offset %d)", total, offset)

        if len(batch.data) < 1000:
            break
        offset += 1000

    logger.info("Backfill complete: %d listings scored", total)


if __name__ == "__main__":
    main()
