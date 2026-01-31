"""SQLite database setup and table creation."""

import aiosqlite
import os

DB_PATH = os.environ.get("JOBPULSE_DB", "jobpulse.db")


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    return db


async def init_db():
    """Create tables on startup if they don't exist."""
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS job_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT NOT NULL,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT,
                salary_min INTEGER,
                salary_max INTEGER,
                currency TEXT,
                tags TEXT DEFAULT '[]',
                url TEXT,
                posted_at TEXT,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                consecutive_misses INTEGER DEFAULT 0,
                quality_score INTEGER DEFAULT 0,
                UNIQUE(external_id, source)
            );

            CREATE TABLE IF NOT EXISTS scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                quality_score REAL DEFAULT 0,
                total_count INTEGER DEFAULT 0,
                added_count INTEGER DEFAULT 0,
                removed_count INTEGER DEFAULT 0,
                retained_count INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_listings_source ON job_listings(source);
            CREATE INDEX IF NOT EXISTS idx_listings_active ON job_listings(is_active);
            CREATE INDEX IF NOT EXISTS idx_runs_source ON scrape_runs(source);
        """)
        await db.commit()
    finally:
        await db.close()
