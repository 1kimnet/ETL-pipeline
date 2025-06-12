from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True, frozen=True)
class Source:
    name: str
    type: str
    url: str
    enabled: bool = True


@dataclass(slots=True, frozen=True)
class Config:
    download_dir: Path
    staging_dir: Path
    sde_path: Path
