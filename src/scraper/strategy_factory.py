"""Strategy factory — loads the correct scraper strategy by source name.

Mirrors Kivaro's create_strategy() pattern: a config-driven factory that
maps source names to concrete strategy classes, enabling new sources to
be added with only a JSON config and a strategy class.
"""

import json
import os
import logging
from src.scraper.base_strategy import BaseScrapeStrategy
from src.scraper.remoteok_strategy import RemoteOKStrategy
from src.scraper.arbeitnow_strategy import ArbeitnowStrategy
from src.scraper.jobicy_strategy import JobicyStrategy
from src.scraper.weworkremotely_strategy import WeWorkRemotelyStrategy
from src.scraper.adzuna_strategy import AdzunaStrategy

logger = logging.getLogger(__name__)

STRATEGY_MAP = {
    "remoteok": RemoteOKStrategy,
    "arbeitnow": ArbeitnowStrategy,
    "jobicy": JobicyStrategy,
    "weworkremotely": WeWorkRemotelyStrategy,
    "adzuna": AdzunaStrategy,
}

CONFIGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "configs")


def create_strategy(source: str) -> BaseScrapeStrategy:
    """Create and return a strategy instance for the given source.

    Loads the JSON config from configs/{source}.json and instantiates
    the matching strategy class.
    """
    config_path = os.path.join(CONFIGS_DIR, f"{source}.json")

    if not os.path.exists(config_path):
        raise ValueError(f"No config found for source '{source}' at {config_path}")

    with open(config_path) as f:
        config = json.load(f)

    strategy_class = STRATEGY_MAP.get(source)
    if not strategy_class:
        raise ValueError(f"Unknown source strategy: '{source}'. Available: {list(STRATEGY_MAP.keys())}")

    logger.info("Created %s strategy for source '%s'", strategy_class.__name__, source)
    return strategy_class(config)
