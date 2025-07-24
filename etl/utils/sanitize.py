# etl/utils/sanitize.py
import re
from typing import Final

_SWEDISH_MAP: Final = str.maketrans("åäöÅÄÖ", "aaoAAO")


def slugify(text: str) -> str:
    """ascii-safe, lower-case, underscore-joined identifier."""
    text = text.translate(_SWEDISH_MAP).lower()
    text = re.sub(r"[^\w\-]+", "_", text)      # keep letters, digits, _
    return re.sub(r"__+", "_", text).strip("_") or "unnamed"
    # collapse repeats, strip leading/trailing _; return "unnamed" if empty
