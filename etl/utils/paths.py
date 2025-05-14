from pathlib import Path
from typing import Final


class paths:  # pylint: disable=too-few-public-methods
    """Centralised data folder layout."""  # noqa: D400

    ROOT: Final[Path] = Path.cwd()
    DATA: Final[Path] = ROOT / "data"
    DOWNLOADS: Final[Path] = DATA / "downloads"
    STAGING: Final[Path] = DATA / "staging"
    GDB: Final[Path] = DATA / "staging.gdb"


def ensure_dirs() -> None:
    for d in (paths.DOWNLOADS, paths.STAGING):
        d.mkdir(parents=True, exist_ok=True)
