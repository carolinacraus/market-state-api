# market_pipeline/pipeline.py
from __future__ import annotations

import os

from market_pipeline.config import PipelineConfig
from market_pipeline.fetcher import DataFetcher
from market_pipeline.breadth import MarketBreadthFetcher
from market_pipeline.merger import CsvMerger
from market_pipeline.indicator import IndicatorCalculator
from scripts.logger import get_logger


class DataPipeline:
    """
    High-level orchestration for the data pipeline.

    Historical:
      - Fetch full market data
      - Fetch full breadth
      - Merge breadth → market
      - Calculate indicators
      - Merge indicators → market

    Daily:
      - Fetch incremental market data
      - Fetch incremental breadth
      - Merge breadth → market
      - Calculate indicators
      - Merge indicators → market
    """

    def __init__(self, cfg: PipelineConfig | None = None, logger=None):
        self.cfg = cfg or PipelineConfig.from_env()
        self.logger = logger or get_logger(__name__)

        # components
        self.fetcher = DataFetcher(cfg=self.cfg, logger=self.logger)
        self.breadth = MarketBreadthFetcher(cfg=self.cfg, logger=self.logger)
        self.ind_calc = IndicatorCalculator(cfg=self.cfg, logger=self.logger)

    # ---------- Public API ----------

    def run_historical(self) -> None:
        """Run a full historical build from scratch."""
        self.logger.info("🚀 Starting historical pipeline")

        # 1) Market data (full)
        self.fetcher.fetch_historical()

        # 2) Breadth (full) + merge → market
        self.breadth.historical()
        self._merge_into_market(self.cfg.breadth_path)

        # 3) Indicators + merge → market
        self.ind_calc.run()
        self._merge_into_market(self.cfg.indicator_path)

        self.logger.info("✅ Historical pipeline complete")

    def run_daily(self) -> None:
        """Run a daily increment-only update."""
        self.logger.info("⏳ Starting daily pipeline")

        if not os.path.exists(self.cfg.market_path):
            self.logger.error(
                f"{self.cfg.market_path!r} not found; run historical pipeline first."
            )
            return

        # 1) Market data (incremental)
        self.fetcher.fetch_daily()

        # 2) Breadth (incremental) + merge → market
        self.breadth.daily()
        self._merge_into_market(self.cfg.breadth_path)

        # 3) Indicators + merge → market
        self.ind_calc.run()
        self._merge_into_market(self.cfg.indicator_path)

        self.logger.info("✅ Daily pipeline complete")

    def run_once(self) -> None:
        """
        Convenience: choose historical or daily based on whether market_path exists.
        Useful if you want a single entrypoint (e.g., Flask route or CLI).
        """
        if not os.path.exists(self.cfg.market_path):
            self.run_historical()
        else:
            self.run_daily()

    # ---------- Internals ----------

    def _merge_into_market(self, merge_path: str) -> None:
        """
        Merge a CSV onto the market CSV on 'Date', dropping overlapping columns from base.
        """
        merger = CsvMerger(self.cfg.market_path, merge_path, logger=self.logger)
        merger.merge()
