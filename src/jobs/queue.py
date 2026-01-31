"""Async job queue for long-running scrape operations.

Mirrors Kivaro's api_server.py job queue: POST starts a background task,
returns a job_id immediately, and the client polls for completion.
In-memory storage with job state transitions: queued → running → completed/failed.
"""

import asyncio
import uuid
import logging
from datetime import datetime
from typing import Dict, Optional, Callable, Coroutine

logger = logging.getLogger(__name__)

_jobs: Dict[str, dict] = {}


def _now() -> str:
    return datetime.utcnow().isoformat()


async def enqueue(coro_factory: Callable[[], Coroutine]) -> str:
    """Queue a coroutine for background execution. Returns job_id."""
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "created_at": _now(),
        "started_at": None,
        "completed_at": None,
        "error": None,
        "result": None,
    }
    asyncio.create_task(_run(job_id, coro_factory))
    logger.info("Job %s queued", job_id)
    return job_id


async def _run(job_id: str, coro_factory: Callable[[], Coroutine]):
    """Execute the job and update its state."""
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = _now()
    logger.info("Job %s running", job_id)

    try:
        result = await coro_factory()
        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["result"] = result
        _jobs[job_id]["completed_at"] = _now()
        logger.info("Job %s completed", job_id)
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = str(e)
        _jobs[job_id]["completed_at"] = _now()
        logger.error("Job %s failed: %s", job_id, e)


def get_status(job_id: str) -> Optional[dict]:
    """Get the current status of a job."""
    return _jobs.get(job_id)


def list_recent(limit: int = 50) -> list:
    """List the most recent jobs."""
    jobs = sorted(_jobs.values(), key=lambda j: j["created_at"], reverse=True)
    return jobs[:limit]
