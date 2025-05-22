# ETL-pipeline/etl/models.py
from __future__ import annotations

import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional 
import logging
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
    download_format: Optional[str] = None
    staged_data_type: Optional[str] = None
    include: List[str] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict) # For any other custom attributes

    @classmethod
    def from_dict(cls, dct: Dict[str, Any]) -> "Source":
        dct_copy = dct.copy()

        # Fields that are directly part of the dataclass definition (excluding 'raw' for now)
        known_field_names = {f.name for f in cls.__dataclass_fields__.values() if f.name != 'raw'}
        
        init_args = {}
        
        # Handle 'include' parsing specifically if present
        if "include" in dct_copy:
            init_args["include"] = _parse_include(dct_copy.pop("include"))

        # Extract explicit 'raw' dictionary from YAML if it exists
        explicit_raw_dict_from_yaml = dct_copy.pop('raw', None)

        # Populate init_args for known fields and collect other items for the raw dictionary
        unconsumed_yaml_items_for_raw = {}
        for k, v in dct_copy.items():
            if k in known_field_names:
                init_args[k] = v
            else:
                # This key is not a defined field in the dataclass (other than 'raw' or 'include')
                unconsumed_yaml_items_for_raw[k] = v
        
        # Instantiate the object. obj.raw will be an empty dict due to default_factory.
        obj = cls(**init_args)

        # Now, correctly populate obj.raw
        # 1. Start with the dictionary from the 'raw' key in YAML, if it was a dict.
        if isinstance(explicit_raw_dict_from_yaml, dict):
            obj.raw.update(explicit_raw_dict_from_yaml)
        elif explicit_raw_dict_from_yaml is not None:
            # 'raw' key was present in YAML but wasn't a dictionary. Log a warning.
            logging.warning(
                f"Source '{dct.get('name', 'Unknown')}' has a 'raw' field in YAML which is not a dictionary. "
                f"Content: {explicit_raw_dict_from_yaml}. This content will be ignored for obj.raw."
            )
        
        # 2. Add any other top-level keys from YAML that weren't direct dataclass fields.
        obj.raw.update(unconsumed_yaml_items_for_raw)
        
        return obj

    @staticmethod
    def load_all(path: Path) -> List["Source"]:
        """Loads all sources from a YAML file."""
        try:
            with path.open("r", encoding="utf-8") as fh:
                doc = yaml.safe_load(fh)
            if doc is None or "sources" not in doc or not isinstance(doc["sources"], list):
                logging.error(f"❌ YAML file {path} is empty or not in expected format (missing 'sources' list).")
                return []
            return [Source.from_dict(d) for d in doc["sources"]]
        except FileNotFoundError:
            logging.error(f"❌ Sources YAML file not found: {path}")
            return []
        except yaml.YAMLError as ye:
            logging.error(f"❌ Error parsing sources YAML file {path}: {ye}", exc_info=True)
            return []
        except Exception as e:
            logging.error(f"❌ Unexpected error loading sources from {path}: {e}", exc_info=True)
            return []