from __future__ import annotations

from pathlib import Path
from typing import Iterable

import yaml

from mini_etl.models import Config, Source


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_config(path: Path) -> Config:
    data = load_yaml(path)
    return Config(
        download_dir=Path(data["download_dir"]),
        staging_dir=Path(data["staging_dir"]),
        sde_path=Path(data["sde_path"]),
    )


def load_sources(path: Path) -> Iterable[Source]:
    for item in load_yaml(path).get("sources", []):
        yield Source(
            name=item["name"],
            type=item["type"],
            url=item["url"],
            enabled=item.get("enabled", True),
        )
