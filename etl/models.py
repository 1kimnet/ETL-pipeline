from __future__ import annotations

import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional # Added Optional
import logging
import yaml


def _parse_include(value: Any) -> List[str]:
    """Return a clean list no matter how the YAML was written."""
    if value is None:
        return []

    raw_items = value if isinstance(value, list) else [value]
    out: List[str] = []
    for item in raw_items:
        for part in re.split(r"[;,]", str(item)): # Handles comma or semicolon separated
            cleaned = part.strip().rstrip(".") # Strip whitespace and trailing dots
            if cleaned:
                out.append(cleaned)
    return out


@dataclass
class Source:
    """Object representation of one entry in *sources.yaml*."""

    name: str
    authority: str
    type: str = "file"  # e.g., "file", "atom_feed", "rest_api", "gpkg_file"
    url: str = ""
    enabled: bool = True
    
    # download_format: Can be used by handlers to know if it's zip, gpkg direct, etc.
    download_format: Optional[str] = None 
    
    # staged_data_type: Hints to the loader how to process staged files.
    # e.g., "shapefile_collection", "gpkg"
    staged_data_type: Optional[str] = None 
    
    # include: For "shapefile_collection" (from zip), it's archive stems.
    #          For "gpkg", it's feature class names (after 'main.' stripping).
    include: List[str] = field(default_factory=list)
    
    raw: Dict[str, Any] = field(default_factory=dict) # For any other custom attributes

    @classmethod
    def from_dict(cls, dct: Dict[str, Any]) -> "Source":
        dct = dct.copy() # Work on a copy
        
        # Ensure 'include' is processed by _parse_include
        if "include" in dct:
            dct["include"] = _parse_include(dct["include"])
        
        # Get known field names from the dataclass definition
        known_field_names = {f.name for f in cls.__dataclass_fields__.values()}
        
        # Prepare arguments for dataclass instantiation (only known fields)
        init_args = {k: v for k, v in dct.items() if k in known_field_names}
        
        # Store any extra fields in the 'raw' dictionary
        extra_args = {k: v for k, v in dct.items() if k not in known_field_names}
        
        # Instantiate the object
        obj = cls(**init_args)
        obj.raw = extra_args # Assign extra arguments to the 'raw' field
        
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

