"""Register all concrete handlers in a single mapping used by Pipeline."""

from typing import Dict, Type

# from ..models import Source # Source model not directly needed here
from .file import FileDownloadHandler # Updated class name

HANDLER_MAP: Dict[str, Type[FileDownloadHandler]] = { # Updated type hint
    "file": FileDownloadHandler,
    # Add other handlers here as they are created, e.g.:
    # "atom_feed": AtomFeedHandler,
    # "rest_api": RestApiHandler,
}

__all__ = ["HANDLER_MAP"]
