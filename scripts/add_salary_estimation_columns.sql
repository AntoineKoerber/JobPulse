-- Add salary estimation columns to job_listings
-- Run this in the Supabase SQL Editor before deploying the estimation feature

ALTER TABLE job_listings
  ADD COLUMN IF NOT EXISTS salary_estimated         BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS salary_confidence        REAL,
  ADD COLUMN IF NOT EXISTS salary_estimation_method TEXT,
  ADD COLUMN IF NOT EXISTS salary_estimated_at      TIMESTAMPTZ;
