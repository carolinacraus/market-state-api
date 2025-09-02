# market_pipeline/indicator.py
from __future__ import annotations

import os

from market_pipeline.config import PipelineConfig
from scripts.calculate_indicators import calculate_all_indicators
from scripts.logger import get_logger


class IndicatorCalculator:
    """
    Orchestrates indicator computation using the shared config.
    Single responsibility: read from market CSV, write enriched indicators CSV.
    """

    def __init__(self, cfg: PipelineConfig | None = None, logger=None):
        self.cfg = cfg or PipelineConfig.from_env()
        self.logger = logger or get_logger(__name__)
        # Ensure directories exist (paired with fetcher's ensure_dirs)
        os.makedirs(self.cfg.data_dir, exist_ok=True)

    def run(
        self,
        *,
        rsi_window: int = 14,
        roc_window: int = 10,
        slope_window: int = 20,
        sma_window: int = 3,
        bbw_window: int = 20,
    ) -> None:
        """
        Compute indicators from cfg.market_path → cfg.indicator_path.
        Tunable windows are exposed as kwargs for experiments.
        """
        self.logger.info("▶ Running IndicatorCalculator")
        calculate_all_indicators(
            self.cfg.market_path,
            self.cfg.indicator_path,
            logger=self.logger,
            rsi_window=rsi_window,
            roc_window=roc_window,
            slope_window=slope_window,
            sma_window=sma_window,
            bbw_window=bbw_window,
        )
        self.logger.info(f"✅ Indicators written to {self.cfg.indicator_path}")
