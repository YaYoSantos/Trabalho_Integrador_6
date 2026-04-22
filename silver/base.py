"""Abstract base class for Silver normalizers."""
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class BaseNormalizer(ABC):
    """Reads Bronze artifacts, normalizes them, and persists Silver CSVs."""

    def __init__(self, bronze_dir: Path, silver_dir: Path) -> None:
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        silver_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def normalize(self) -> pd.DataFrame:
        """Return the normalized DataFrame and write it to silver_dir."""
