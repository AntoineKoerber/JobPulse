-- Add attractiveness score for smart listing ranking
-- Run this in Supabase SQL editor

ALTER TABLE job_listings
  ADD COLUMN IF NOT EXISTS attractiveness_score SMALLINT DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_job_listings_attractiveness
  ON job_listings (attractiveness_score DESC)
  WHERE is_active = TRUE;
