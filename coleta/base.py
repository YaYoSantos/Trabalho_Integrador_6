"""Abstract base classes for the collection layer (Bronze)."""
from abc import ABC, abstractmethod
from pathlib import Path


class BaseCollector(ABC):
    """Fetches raw data from an external source and persists it to Bronze."""

    def __init__(self, bronze_dir: Path) -> None:
        self.bronze_dir = bronze_dir
        bronze_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def collect(self) -> None:
        """Run the full collection and save every artifact to bronze_dir."""
