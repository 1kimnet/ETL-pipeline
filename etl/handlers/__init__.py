# handlers/__init__.py
from __future__ import annotations
from typing import Dict, Type

from .file import FileDownloadHandler
from .atom_feed import AtomFeedDownloadHandler   # ← your new class

HANDLER_MAP: Dict[str, Type] = {
    "file": FileDownloadHandler,        # ZIP, GPKG, etc.
    "atom_feed": AtomFeedDownloadHandler,
    # "rest_api": RestApiDownloadHandler,   # future
    # "ogc_api":  OgcApiDownloadHandler,    # future
}

__all__ = ["HANDLER_MAP"]