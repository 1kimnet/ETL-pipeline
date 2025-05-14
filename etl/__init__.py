"""Lightweight ETL package – expose a single convenience *run()* function."""
from pathlib import Path
from typing import Any, Dict

from .pipeline import Pipeline  # lazy import

def run(sources: str | Path = "sources.yaml", **kwargs: Dict[str, Any]) -> None:
    """Run the whole pipeline (mainly for notebooks / interactive use)."""
    Pipeline(Path(sources), **kwargs).run()

__all__ = ["run", "Pipeline"]
