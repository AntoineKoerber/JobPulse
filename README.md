# JobPulse

A resilient job listing scraper with async data pipeline, quality validation, and analytics dashboard. Built to demonstrate production-grade engineering patterns applied to real-world job market data.

## Architecture

```
                          +------------------+
                          |   Dashboard UI   |
                          |  (Charts, Table) |
                          +--------+---------+
                                   |
                            REST API (FastAPI)
                    +------+-------+--------+------+
                    |              |               |
              POST /scrape   GET /jobs       GET /trends
                    |              |               |
             +------v------+  +---v----+   +------v------+
             | Job Queue   |  | SQLite |   |  Analytics  |
             | (async)     |  | Query  |   |  Engine     |
             +------+------+  +--------+   +-------------+
                    |
        +-----------+-----------+
        |                       |
  +-----v------+        +------v------+
  | RemoteOK   |        |  Arbeitnow  |
  | Strategy   |        |  Strategy   |
  +-----+------+        +------+------+
        |                       |
        +--------+------+------+
                 |      |      |
           Humanizer  Normalizer  Validator
                         |
                   +-----v------+
                   |  Resilience |
                   |  Pipeline   |
                   +------+------+
                   |      |      |
              Fallback  Change  Stability
              System    Detect  Tracker
```

## Engineering Patterns

| Pattern | Description |
|---------|-------------|
| **Strategy Pattern** | Abstract base class + concrete implementations per job board. New sources require only a JSON config and strategy class. |
| **Config-Driven Scraping** | Each source is defined by a JSON config with field mappings, rate limits, and headers. No code changes needed to adjust behavior. |
| **Async Job Queue** | POST starts background scrape, returns job ID immediately. Client polls for completion. Prevents HTTP timeouts on long operations. |
| **Quality Validation** | Each listing scored 0-100 on field completeness. Scrape-level score triggers retry or fallback if below threshold. |
| **Fallback System** | Failed or low-quality scrapes return the last known good data instead of serving bad results. |
| **Stability Tracking** | Listings must be missing for 3 consecutive scrapes before confirmed as removed. Prevents false removals from transient API issues. |
| **Change Detection** | Each scrape computes added/removed/retained diffs against the previous run. Feeds trend analysis. |
| **Multi-Layer Normalization** | Title abbreviation expansion, company name cleaning, location standardization, salary parsing, tag deduplication. |
| **Humanized Rate Limiting** | Jittered delays between requests with escalation on retries, mimicking organic access patterns. |

## Quick Start

```bash
# Clone
git clone https://github.com/AntoineKoerber/JobPulse.git
cd JobPulse

# Install
pip install -r requirements.txt

# Configure (copy and fill in your Supabase keys)
cp .env.example .env

# Run
uvicorn src.main:app --reload

# Open dashboard
open http://localhost:8000
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/scrape` | Start async scrape job. Body: `{"sources": ["remoteok", "arbeitnow"]}` |
| `GET` | `/api/scrape/{job_id}` | Poll job status. Returns `queued`, `running`, `completed`, or `failed`. |
| `GET` | `/api/jobs` | Query listings. Params: `source`, `location`, `role`, `salary_min`, `page`, `limit` |
| `GET` | `/api/trends` | Aggregated insights: top tags, salary distribution, top companies, scrape history |
| `GET` | `/api/health` | Health check with active job count |

## Adding a New Source

1. Create `configs/newsource.json` with field mappings
2. Create `src/scraper/newsource_strategy.py` extending `BaseScrapeStrategy`
3. Register in `strategy_factory.py`

No other code changes needed.

## Running Tests

```bash
pytest tests/ -v
```

## Tech Stack

- **Python** + **FastAPI** — Async API server
- **Supabase** (PostgreSQL) — Cloud-hosted persistent storage
- **httpx** — Async HTTP client
- **Chart.js** — Dashboard visualizations
- **Pydantic** — Data validation and schemas

## License

MIT
