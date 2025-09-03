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
    ) -> bool:
        """
        Compute indicators from cfg.market_path → cfg.indicator_path.
        Returns True if the output file changed.
        """
        self.logger.info("Running IndicatorCalculator")
        # load old for compare (if exists)
        old = None
        if os.path.exists(self.cfg.indicator_path):
            try:
                old = pd.read_csv(self.cfg.indicator_path, nrows=5)  # light touch
            except Exception:
                old = None

        from scripts.calculate_indicators import calculate_all_indicators
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

        # naïve change check: file exists and (size or header) changed
        try:
            if not os.path.exists(self.cfg.indicator_path):
                return False
            new_size = os.path.getsize(self.cfg.indicator_path)
            if new_size == 0:
                return False
            if old is None:
                return True
            new_head = pd.read_csv(self.cfg.indicator_path, nrows=5)
            return list(new_head.columns) != list(old.columns) or len(new_head) != len(old)
        except Exception:
            return True
