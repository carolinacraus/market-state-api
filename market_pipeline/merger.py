# market_pipeline/merger.py
from __future__ import annotations

import os
from typing import Iterable

import pandas as pd

from scripts.logger import get_logger


class CsvMerger:
    """
    Generic CSV-on-Date merger:
      - Drops overlapping columns (except 'Date') from base
      - Left merges merge_path onto base_path
      - Sorts by Date and writes back to base_path
    """

    def __init__(self, base_path: str, merge_path: str, *, logger=None):
        self.base_path = base_path
        self.merge_path = merge_path
        self.logger = logger or get_logger(__name__)

    def merge(self) -> None:
        if not (os.path.exists(self.base_path) and os.path.exists(self.merge_path)):
            self.logger.error(f"Missing file(s): {self.base_path!r} or {self.merge_path!r}")
            return

        df_base = pd.read_csv(self.base_path, parse_dates=["Date"])
        df_merge = pd.read_csv(self.merge_path, parse_dates=["Date"])

        if df_merge.empty:
            self.logger.info(f"No rows in merge source: {self.merge_path}")
            return

        # Drop any columns in base that would collide with merge (except Date)
        overlap = (set(df_base.columns) & set(df_merge.columns)) - {"Date"}
        if overlap:
            df_base.drop(columns=list(overlap), inplace=True)

        df_out = df_base.merge(df_merge, on="Date", how="left")
        df_out.sort_values("Date", inplace=True)
        df_out.to_csv(self.base_path, index=False)

        self.logger.info(
            f"✅ Merged {os.path.basename(self.merge_path)} "
            f"into {os.path.basename(self.base_path)} "
            f"→ rows={len(df_out)}, cols={len(df_out.columns)}"
        )
