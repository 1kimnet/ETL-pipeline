# ETL-pipeline/etl/models.py
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import yaml

log: Final = logging.getLogger(__name__)


def _parse_include(value: Any) -> list[str]:
    """Return a clean list no matter how the YAML was written."""
    if value is None:
        return []

    raw_items = value if isinstance(value, list) else [value]
    out: list[str] = []
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
    staged_data_type: str | None = None
    include: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, dct: dict[str, Any]) -> Source:
        """🔄 Create Source instance from dictionary."""
        dct_copy = dct.copy()

        # Fields that are directly part of the dataclass definition (excluding 'raw')
        known_field_names = {
            f.name for f in cls.__dataclass_fields__.values() if f.name != "raw"
        }

        init_args = {}

        # Handle 'include' parsing specifically if present
        if "include" in dct_copy:
            init_args["include"] = _parse_include(dct_copy.pop("include"))

        # Extract explicit 'raw' dictionary from YAML if it exists
        explicit_raw_dict_from_yaml = dct_copy.pop("raw", None)

        # Populate init_args for known fields and collect other items for raw dict
        unconsumed_yaml_items_for_raw = {}
        for k, v in dct_copy.items():
            if k in known_field_names:
                init_args[k] = v
            else:
                unconsumed_yaml_items_for_raw[k] = v

        # Instantiate the object. obj.raw will be an empty dict due to default_factory
        obj = cls(**init_args)

        # Now, correctly populate obj.raw
        # 1. Start with the dictionary from the 'raw' key in YAML, if it was a dict
        if isinstance(explicit_raw_dict_from_yaml, dict):
            obj.raw.update(explicit_raw_dict_from_yaml)
        elif explicit_raw_dict_from_yaml is not None:
            # 'raw' key was present in YAML but wasn't a dictionary. Log a warning
            log.warning(
                "⚠️ Source '%s' has a 'raw' field in YAML which is not a dictionary. "
                "Content: %s. This content will be ignored for obj.raw.",
                dct.get("name", "Unknown"),
                explicit_raw_dict_from_yaml,
            )

        # 2. Add any other top-level keys from YAML that weren't direct dataclass fields
        obj.raw.update(unconsumed_yaml_items_for_raw)

        return obj

    @staticmethod
    def load_all(path: Path | str) -> list[Source]:
        """🔄 Load all sources from a YAML file."""
        # Convert to Path and resolve relative to current working directory
        yaml_path = Path(path).resolve()
        
        try:
            log.info("🔄 Loading sources from: %s", yaml_path.relative_to(Path.cwd()))
            
            with yaml_path.open("r", encoding="utf-8") as fh:
                content = fh.read()
                
            # Debug: show first 200 chars of file content
            log.debug("📄 YAML content preview: %s", content[:200] + "..." if len(content) > 200 else content)
            
            doc = yaml.safe_load(content)
                
            if doc is None:
                log.error("❌ YAML file %s is empty", yaml_path.name)
                return []
                
            if not isinstance(doc, dict):
                log.error(
                    "❌ YAML file %s root is not a dictionary. Got type: %s. Content: %s",
                    yaml_path.name,
                    type(doc).__name__,
                    str(doc)[:100] + "..." if len(str(doc)) > 100 else str(doc)
                )
                return []
                
            if "sources" not in doc:
                log.error(
                    "❌ YAML file %s is missing 'sources' key. Available keys: %s",
                    yaml_path.name,
                    list(doc.keys()),
                )
                return []
                
            sources_data = doc["sources"]
            if not isinstance(sources_data, list):
                log.error(
                    "❌ 'sources' in %s is not a list, got: %s",
                    yaml_path.name,
                    type(sources_data).__name__,
                )
                return []
                
            sources = [Source.from_dict(d) for d in sources_data]
            log.info("✅ Loaded %d sources from %s", len(sources), yaml_path.name)
            return sources
            
        except FileNotFoundError:
            log.error("❌ Sources YAML file not found: %s", yaml_path)
            return []
        except yaml.YAMLError as ye:
            log.error("❌ YAML parsing error in %s: %s", yaml_path.name, ye)
            return []
        except Exception as e:
            log.error("❌ Unexpected error loading sources from %s: %s", yaml_path.name, e)
            return []