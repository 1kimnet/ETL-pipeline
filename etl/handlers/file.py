from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from ..models import Source
from ..utils import download, ensure_dirs, extract_zip, paths


class FileShapefileHandler:  # noqa: D101
    def __init__(self, src: Source):
        self.src = src
        ensure_dirs()

    def fetch(self) -> None:  # noqa: D401
        if self.src.include:
            logging.info("🔗 Concatenation mode (%d archives)…", len(self.src.include))
            for stem in self._iter_includes():
                self._download_one(stem)
        else:
            self._download_one(None)

    # ---------------------------------------------------------------- internals

    def _iter_includes(self) -> Iterable[str]:
        ext = f".{self.src.download_format or 'zip'}"
        for stem in self.src.include:
            yield stem if stem.lower().endswith(ext) else stem + ext

    def _download_one(self, filename: str | None) -> None:
        base = self.src.url.rstrip("/") + "/"
        filepart = filename or Path(base).name
        url = base + filepart if filename else self.src.url

        archive = paths.DOWNLOADS / filepart
        stagedir = paths.STAGING / self.src.authority / Path(filepart).stem

        # --- GPKG direct handling ------------------------------------------
        if url.lower().endswith(".gpkg"):
            target_path = stagedir / f"{Path(filepart).stem}.gpkg"
            downloaded = download(url, target_path)
            logging.info("➕ Staged %s", target_path.relative_to(paths.ROOT))
            return

        downloaded = download(url, archive)
        extract_zip(downloaded, stagedir)
        logging.info("➕ Staged %s", stagedir.relative_to(paths.ROOT))
