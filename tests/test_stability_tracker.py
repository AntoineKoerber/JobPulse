"""Tests for the stability tracker (consecutive miss confirmation)."""

import pytest
import pytest_asyncio
import aiosqlite
from src.resilience.stability_tracker import update_stability, REMOVAL_THRESHOLD


@pytest_asyncio.fixture
async def db():
    conn = await aiosqlite.connect(":memory:")
    await conn.executescript("""
        CREATE TABLE job_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT NOT NULL,
            source TEXT NOT NULL,
            title TEXT DEFAULT '',
            company TEXT DEFAULT '',
            location TEXT,
            salary_min INTEGER,
            salary_max INTEGER,
            currency TEXT,
            tags TEXT DEFAULT '[]',
            url TEXT,
            posted_at TEXT,
            first_seen TEXT DEFAULT '',
            last_seen TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            consecutive_misses INTEGER DEFAULT 0,
            quality_score INTEGER DEFAULT 0,
            UNIQUE(external_id, source)
        );
    """)
    await conn.commit()
    yield conn
    await conn.close()


async def insert_listing(db, ext_id, source="test", misses=0):
    await db.execute(
        """INSERT INTO job_listings (external_id, source, first_seen, last_seen, consecutive_misses, is_active)
           VALUES (?, ?, '', '', ?, 1)""",
        (ext_id, source, misses),
    )
    await db.commit()


@pytest.mark.asyncio
async def test_found_resets_misses(db):
    await insert_listing(db, "job1", misses=2)
    result = await update_stability(db, "test", {"job1"})
    assert result["confirmed_removals"] == set()
    assert result["tentative_removals"] == set()

    cursor = await db.execute("SELECT consecutive_misses FROM job_listings WHERE external_id = 'job1'")
    row = await cursor.fetchone()
    assert row[0] == 0


@pytest.mark.asyncio
async def test_missing_increments(db):
    await insert_listing(db, "job1", misses=0)
    result = await update_stability(db, "test", set())  # job1 not found
    assert "job1" in result["tentative_removals"]


@pytest.mark.asyncio
async def test_confirmed_removal_at_threshold(db):
    await insert_listing(db, "job1", misses=REMOVAL_THRESHOLD - 1)
    result = await update_stability(db, "test", set())
    assert "job1" in result["confirmed_removals"]

    cursor = await db.execute("SELECT is_active FROM job_listings WHERE external_id = 'job1'")
    row = await cursor.fetchone()
    assert row[0] == 0
