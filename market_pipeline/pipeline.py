# market_pipeline/pipeline.py
from __future__ import annotations

import os

from market_pipeline.config import PipelineConfig
from market_pipeline.fetcher import DataFetcher
from market_pipeline.breadth import MarketBreadthFetcher
from market_pipeline.merger import CsvMerger
from market_pipeline.indicator import IndicatorCalculator
from scripts.logger import get_logger
from datetime import datetime
from scripts.github_upload import upload_if_changed  # <- new


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

    def _upload_changed(self, changed_paths: list[str], mode_label: str) -> None:
        """Upload only the files that changed."""
        if not changed_paths:
            self.logger.info("No file changes detected; skipping GitHub upload.")
            return
        repo = os.getenv("GITHUB_REPO", self.cfg.repo)
        branch = os.getenv("GITHUB_BRANCH")  # optional; default repo default branch
        stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        prefix = os.getenv("COMMIT_PREFIX", "Pipeline")
        for local in changed_paths:
            if not os.path.exists(local):
                continue
            remote = f"data/{os.path.basename(local)}"
            msg = f"{prefix}: {mode_label} update @ {stamp}"
            ok, ref, changed = upload_if_changed(local, repo, remote, msg, branch=branch)
            if ok and changed:
                self.logger.info(f"Uploaded {remote} (ref={ref[:7] if ref else ''})")
            elif ok:
                self.logger.info(f"No change for {remote}; not uploaded.")
            else:
                self.logger.error(f"Upload failed for {remote}: {ref}")

    def run_historical(self) -> None:
        self.logger.info("Starting historical pipeline")
        changed = []

        if self.fetcher.fetch_historical():
            changed.append(self.cfg.market_path)

        self.breadth.historical()
        if CsvMerger(self.cfg.market_path, self.cfg.breadth_path, logger=self.logger).merge():
            changed.append(self.cfg.market_path)

        if self.ind_calc.run():
            if CsvMerger(self.cfg.market_path, self.cfg.indicator_path, logger=self.logger).merge():
                changed.append(self.cfg.market_path)
            changed.append(self.cfg.indicator_path)

        # Always consider breadth file potentially new after historical
        if os.path.exists(self.cfg.breadth_path):
            changed.append(self.cfg.breadth_path)

        self._upload_changed(sorted(set(changed)), "historical")
        self.logger.info("Historical pipeline complete")

    def run_daily(self) -> None:
        self.logger.info("Starting daily pipeline")
        changed = []

        if self.fetcher.fetch_daily():
            changed.append(self.cfg.market_path)

        self.breadth.daily()
        if CsvMerger(self.cfg.market_path, self.cfg.breadth_path, logger=self.logger).merge():
            changed.append(self.cfg.market_path)
        if os.path.exists(self.cfg.breadth_path):
            changed.append(self.cfg.breadth_path)

        if self.ind_calc.run():
            if CsvMerger(self.cfg.market_path, self.cfg.indicator_path, logger=self.logger).merge():
                changed.append(self.cfg.market_path)
            if os.path.exists(self.cfg.indicator_path):
                changed.append(self.cfg.indicator_path)

        self._upload_changed(sorted(set(changed)), "daily")
        self.logger.info("Daily pipeline complete")

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
