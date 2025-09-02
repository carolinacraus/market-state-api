# market_pipeline/breadth.py
from __future__ import annotations

from market_pipeline.config import PipelineConfig
from scripts.market_breadth import gather_historical, daily_update_function
from scripts.logger import get_logger


class MarketBreadthFetcher:
    """
    Thin orchestrator that wraps the breadth script functions so the pipeline
    can call .historical() or .daily() with shared config.
    """

    def __init__(self, cfg: PipelineConfig | None = None, logger=None):
        self.cfg = cfg or PipelineConfig.from_env()
        self.logger = logger or get_logger(__name__)

    def historical(self) -> None:
        self.logger.info("▶ Running breadth historical fetch")
        gather_historical(cfg=self.cfg, logger=self.logger)

    def daily(self) -> None:
        self.logger.info("▶ Running breadth daily update")
        daily_update_function(cfg=self.cfg, logger=self.logger)
