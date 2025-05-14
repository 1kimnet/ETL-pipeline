"""Register all concrete handlers in a single mapping used by Pipeline."""

from typing import Dict, Type

from ..models import Source
from .file import FileShapefileHandler

HANDLER_MAP: Dict[str, Type] = {
    "file": FileShapefileHandler,
}

__all__ = ["HANDLER_MAP"]