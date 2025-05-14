from __future__ import annotations

import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List

import yaml


def _parse_include(value: Any) -> List[str]:
    """Return a clean list no matter how the YAML was written."""
    if value is None:
        return []

    raw_items = value if isinstance(value, list) else [value]
    out: List[str] = []
    for item in raw_items:
        for part in re.split(r"[;,]", str(item)):
            cleaned = part.strip().rstrip(".")
            if cleaned:
                out.append(cleaned)
    return out


@dataclass
class Source:
    """Object representation of one entry in *sources.yaml*."""

    name: str
    authority: str
    type: str = "file"
    url: str = ""
    enabled: bool = True
    download_format: str | None = None
    include: List[str] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, dct: Dict[str, Any]) -> "Source":
        dct = dct.copy()
        if "include" in dct:
            dct["include"] = _parse_include(dct["include"])
        known = {f.name for f in cls.__dataclass_fields__.values()}
        extra = {k: v for k, v in dct.items() if k not in known}
        obj = cls(**{k: v for k, v in dct.items() if k in known})
        obj.raw = extra
        return obj

    # Utility: load full list from YAML file
    @staticmethod
    def load_all(path: Path) -> List["Source"]:
        with path.open("r", encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        return [Source.from_dict(d) for d in doc.get("sources", [])]