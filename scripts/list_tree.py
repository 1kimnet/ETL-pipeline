#!/usr/bin/env python
"""
list_tree.py – Render a Unicode “tree” of a workspace.

Default behaviour
-----------------
• Starts at the nearest Git repo root (or the script’s parent if none).
• Lists both directories *and* files.
• Hides build artefacts:  __pycache__/, *.pyc, .git/, and any folder named data/.

Flags
-----
--dirs-only   Suppress files (directories only).
--show-all    Show everything (ignore default exclusions).
path          Optional root directory (overrides auto-detection).

Example
-------
python list_tree.py               # repo tree, artefacts hidden
python list_tree.py ..            # explicit parent folder
python list_tree.py --show-all    # include __pycache__, *.pyc, data/, …
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Final, Iterable

log: Final = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ── tree drawing constants ────────────────────────────────────────────────
VERTICAL: Final[str] = "│   "
BRANCH:   Final[str] = "├── "
LAST:     Final[str] = "└── "

# ── default exclusions ────────────────────────────────────────────────────
EXCLUDED_NAMES: Final[set[str]] = {"__pycache__", ".git", "data"}
EXCLUDED_SUFFIXES: Final[set[str]] = {".pyc"}


# ── helpers ───────────────────────────────────────────────────────────────
def find_git_root(start: Path) -> Path:
    """Return the nearest ancestor containing .git; else *start*."""
    for parent in (start, *start.parents):
        if (parent / ".git").is_dir():
            return parent
    return start


def _is_excluded(path: Path, *, show_all: bool) -> bool:
    """True if *path* should be hidden according to defaults."""
    if show_all:
        return False
    if path.name in EXCLUDED_NAMES:
        return True
    if path.suffix in EXCLUDED_SUFFIXES:
        return True
    return False


def _tree_lines(
    path: Path,
    *,
    prefix: str = "",
    dirs_only: bool = False,
    show_all: bool = False,
) -> Iterable[str]:
    """Yield lines for *path*’s children, subject to flags."""
    children = sorted(
        (p for p in path.iterdir() if not _is_excluded(p, show_all=show_all)),
        key=lambda p: (not p.is_dir(), p.name.lower()),
    )
    if dirs_only:
        children = [c for c in children if c.is_dir()]

    for idx, child in enumerate(children):
        connector = LAST if idx == len(children) - 1 else BRANCH
        yield f"{prefix}{connector}{child.name}" + ("/" if child.is_dir() else "")
        if child.is_dir():
            extension = "    " if connector is LAST else VERTICAL
            yield from _tree_lines(
                child,
                prefix=prefix + extension,
                dirs_only=dirs_only,
                show_all=show_all,
            )


def build_tree(
    root: Path,
    *,
    dirs_only: bool = False,
    show_all: bool = False,
) -> str:
    """🔄 Build a visual tree of *root* and descendants."""
    if not root.exists():
        log.error("❌ Path not found: %s", root)
        raise FileNotFoundError(root)

    lines = [
        f"{root.resolve().name}/",
        *_tree_lines(root, dirs_only=dirs_only, show_all=show_all),
    ]
    log.info("✅ Directory tree generated for %s", root)
    return "\n".join(lines)


# ── CLI entry point ───────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="List workspace directory tree.")
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        help="Root directory (defaults to nearest Git repo root).",
    )
    parser.add_argument(
        "--dirs-only",
        action="store_true",
        help="Show directories only (suppress files).",
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Do NOT exclude build artefacts or data/ folder.",
    )
    args = parser.parse_args()

    root = args.path if args.path else find_git_root(Path.cwd())
    print(build_tree(root, dirs_only=args.dirs_only, show_all=args.show_all))


if __name__ == "__main__":  # pragma: no cover
    main()
