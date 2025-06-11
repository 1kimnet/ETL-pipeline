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


def derive_authority_from_path(file_path: Path, staging_root: Path) -> str:
    """📂 Helper to derive authority from file path structure."""
    try:
        path_parts: tuple[str, ...] = file_path.relative_to(staging_root).parts
        return path_parts[0] if len(path_parts) > 1 else "UNKNOWN_GLOB_AUTH"
    except (IndexError, ValueError):
        return "UNKNOWN_GLOB_AUTH_EXC"
