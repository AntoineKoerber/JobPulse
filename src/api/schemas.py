"""Pydantic models for API request/response schemas."""

from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class RawJobListing(BaseModel):
    """Raw listing as received from a source before normalization."""
    external_id: str
    source: str
    title: str
    company: str
    location: Optional[str] = None
    salary_raw: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    currency: Optional[str] = None
    tags: List[str] = []
    url: Optional[str] = None
    posted_at: Optional[str] = None


class JobListing(BaseModel):
    """Normalized job listing stored in the database."""
    id: Optional[int] = None
    external_id: str
    source: str
    title: str
    company: str
    location: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    currency: Optional[str] = None
    tags: List[str] = []
    url: Optional[str] = None
    posted_at: Optional[str] = None
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    is_active: bool = True
    consecutive_misses: int = 0
    quality_score: int = 0


class ScrapeRequest(BaseModel):
    """Request to start a scrape."""
    sources: List[str] = ["remoteok", "arbeitnow"]


class ScrapeStatusResponse(BaseModel):
    """Response for scrape job status."""
    job_id: str
    status: str  # queued, running, completed, failed
    created_at: str
    completed_at: Optional[str] = None
    error: Optional[str] = None
    result: Optional[dict] = None


class TrendsResponse(BaseModel):
    """Aggregated trends data."""
    scrape_history: List[dict] = []
    top_tags: List[dict] = []
    salary_distribution: List[dict] = []
    top_companies: List[dict] = []
    sources_breakdown: List[dict] = []
