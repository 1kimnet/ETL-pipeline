"""Lightweight ETL package – expose a single convenience *run()* function."""

from pathlib import Path
from typing import Any, Dict

from .pipeline import Pipeline  # noqa: E402 (lazy import)


def run(sources: str | Path = "sources.yaml", **kwargs: Dict[str, Any]) -> None:  # noqa: D401
    """Run the whole pipeline (mainly for notebooks / interactive use)."""
    Pipeline(Path(sources), **kwargs).run()

__all__ = ["run", "Pipeline"]