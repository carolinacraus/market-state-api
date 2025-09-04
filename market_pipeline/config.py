# market_pipeline/config.py

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


# --- Paths ---
BASE_DIR: Path = Path(__file__).resolve().parent.parent
CONFIG_DIR: Path = BASE_DIR / "config"


def _load_yaml_optional(fname: str) -> Dict[str, Any]:
    """Load YAML from config/ if present; return {} if missing."""
    path = CONFIG_DIR / fname
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{fname} must contain a YAML mapping at top level.")
    return data


@dataclass(frozen=True)
class PipelineConfig:
    """
    Central application configuration.
    - File/dir locations may be overridden via env vars.
    - Ticker map and market-state thresholds are loaded from YAML if present.
    """

    # Directories
    data_dir: str = field(default_factory=lambda: os.getenv("DATA_DIR", str(BASE_DIR / "data")))
    logs_dir: str = field(default_factory=lambda: os.getenv("LOGS_DIR", str(BASE_DIR / "logs")))
    repo: str = field(default_factory=lambda: os.getenv("GITHUB_REPO", "carolinacraus/market-state-api"))

    # Filenames (within data_dir)
    market_filename: str = field(default_factory=lambda: os.getenv("MARKET_FILE", "MarketStates_Data.csv"))
    indicator_filename: str = field(default_factory=lambda: os.getenv("INDICATOR_FILE", "MarketData_with_Indicators.csv"))
    breadth_filename: str = field(default_factory=lambda: os.getenv("BREADTH_FILE", "market_breadth.csv"))

    # Date range defaults (can be overridden via env)
    historical_start: str = field(default_factory=lambda: os.getenv("HISTORICAL_START", "2005-01-01"))
    historical_end: str = field(default_factory=lambda: os.getenv(
        "HISTORICAL_END",
        (date.today() - timedelta(days=1)).isoformat()
    ))

    # YAML-driven configuration
    ticker_map: Dict[str, str] = field(default_factory=lambda: _load_yaml_optional("ticker_map.yaml").get("ticker_map", {}))
    market_states: Dict[str, Any] = field(default_factory=lambda: _load_yaml_optional("market_states.yaml"))

    # --- Resolved absolute paths ---
    @property
    def market_path(self) -> str:
        return str(Path(self.data_dir) / self.market_filename)

    @property
    def indicator_path(self) -> str:
        return str(Path(self.data_dir) / self.indicator_filename)

    @property
    def breadth_path(self) -> str:
        return str(Path(self.data_dir) / self.breadth_filename)

    # --- Helpers ---
    def ensure_dirs(self) -> None:
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.logs_dir).mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Factory that reads env overrides (useful in tests/CLI)."""
        return cls()
