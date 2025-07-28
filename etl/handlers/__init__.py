# handlers/__init__.py
from __future__ import annotations

from typing import Any, Dict, Type, TypeAlias

from .atom_feed import AtomFeedDownloadHandler
from .file import FileDownloadHandler
from .ogc_api import OgcApiDownloadHandler
from .rest_api import RestApiDownloadHandler

# Define a more specific type for the handler classes if possible
DownloadHandlerType: TypeAlias = Type[Any]


HANDLER_MAP: Dict[str, DownloadHandlerType] = {
    "file": FileDownloadHandler,
    "atom_feed": AtomFeedDownloadHandler,
    "rest_api": RestApiDownloadHandler,
    "ogc_api": OgcApiDownloadHandler,
}

__all__ = ["HANDLER_MAP"]
