"""Lightweight ETL package – expose a single convenience *run()* function."""

from pathlib import Path
from typing import Any

from .pipeline import Pipeline  # noqa: E402 (lazy import)


def run(sources: str | Path = "sources.yaml", **kwargs: Any) -> None:
    """Run the whole pipeline (mainly for notebooks / interactive use)."""
    Pipeline(sources_yaml=Path(sources), **kwargs).run()


__all__ = ["run", "Pipeline"]