# scripts/data_retrieval.py
from __future__ import annotations

import os

from dotenv import load_dotenv

from market_pipeline.config import PipelineConfig
from market_pipeline.pipeline import DataPipeline
from scripts.logger import get_logger
from datetime import datetime


def main():
    load_dotenv()
    logger = get_logger("data_retrieval")
    cfg = PipelineConfig.from_env()

    # Decide whether to run historical or daily based on file existence
    if not os.path.exists(cfg.market_path):
        logger.info("🆕 MarketStates_Data.csv not found → running full historical pipeline.")
        DataPipeline(cfg=cfg, logger=logger).run_historical()
    else:
        logger.info("📅 MarketStates_Data.csv found → running daily pipeline.")
        DataPipeline(cfg=cfg, logger=logger).run_daily()


if __name__ == "__main__":
    main()
