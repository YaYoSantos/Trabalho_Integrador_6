"""Abstract base class for Gold analyzers."""
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class BaseAnalyzer(ABC):
    """Reads Silver data, computes indicators, and saves Gold artifacts."""

    def __init__(self, silver_dir: Path, gold_dir: Path) -> None:
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir
        gold_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def analyze(self) -> pd.DataFrame:
        """Compute and return the Gold DataFrame, also persisting it to gold_dir."""
