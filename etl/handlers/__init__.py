# handlers/__init__.py
from __future__ import annotations
from typing import Dict, Type, TypeAlias, Any # Added TypeAlias, Any

from .ogc_api import OgcApiDownloadHandler
from .file import FileDownloadHandler
from .atom_feed import AtomFeedDownloadHandler
from .rest_api import RestApiDownloadHandler # <-- Add this

# Define a more specific type for the handler classes if possible
# from .base_handler import BaseDownloadHandler # Assuming you might create a base class
# DownloadHandlerType: TypeAlias = Type[BaseDownloadHandler]
DownloadHandlerType: TypeAlias = Type[Any] # Using Any for now for simplicity

HANDLER_MAP: Dict[str, DownloadHandlerType] = {
    "file": FileDownloadHandler,
    "atom_feed": AtomFeedDownloadHandler,
    "rest_api": RestApiDownloadHandler,
    "ogc_api": OgcApiDownloadHandler,  
}

__all__ = ["HANDLER_MAP"]