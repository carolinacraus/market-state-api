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

    def merge(self) -> bool:
        if not (os.path.exists(self.base_path) and os.path.exists(self.merge_path)):
            self.logger.error(f"Missing file(s): {self.base_path!r} or {self.merge_path!r}")
            return False

        df_base = pd.read_csv(self.base_path, parse_dates=["Date"]).sort_values("Date")
        before_cols = set(df_base.columns)
        before_len = len(df_base)

        df_merge = pd.read_csv(self.merge_path, parse_dates=["Date"]).sort_values("Date")
        if df_merge.empty:
            self.logger.info(f"No rows in merge source: {self.merge_path}")
            return False

        overlap = (set(df_base.columns) & set(df_merge.columns)) - {"Date"}
        if overlap:
            df_base.drop(columns=list(overlap), inplace=True)

        df_out = df_base.merge(df_merge, on="Date", how="left").sort_values("Date")

        changed = (len(df_out) != before_len) or (set(df_out.columns) != before_cols) or (not df_out.equals(df_base))
        if not changed:
            self.logger.info("Merge produced no changes; skipping write.")
            return False

        df_out.to_csv(self.base_path, index=False)
        self.logger.info(
            f"✅ Merged {os.path.basename(self.merge_path)} into {os.path.basename(self.base_path)} "
            f"→ rows={len(df_out)}, cols={len(df_out.columns)}"
        )
        return True
