"""JobPulse — Job listing scraper with resilient data pipeline.

FastAPI application entry point. Serves the API and static frontend.
"""

import logging
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from src.api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI(
    title="JobPulse",
    description="Job listing scraper with resilient data pipeline and analytics dashboard",
    version="1.0.0",
)

app.include_router(router)
app.mount("/static", StaticFiles(directory="frontend"), name="static")


@app.get("/")
async def index():
    return FileResponse("frontend/index.html")
