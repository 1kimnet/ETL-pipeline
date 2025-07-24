# etl/utils/run_summary.py
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import List


@dataclass(slots=True)
class Summary:
    downloads: Counter = field(default_factory=Counter)
    staging: Counter = field(default_factory=Counter)
    sde: Counter = field(default_factory=Counter)  # Add SDE counter
    errors: List[str] = field(default_factory=list)

    # ------------------------------------------------------------------ API
    def log_download(self, status: str) -> None:
        self.downloads[status] += 1

    def log_staging(self, status: str) -> None:
        self.staging[status] += 1

    def log_sde(self, status: str) -> None:
        """🚚 Track SDE loading operations."""
        self.sde[status] += 1

    def log_error(self, src: str, msg: str) -> None:
        if len(self.errors) < 10:
            self.errors.append(f"{src}: {msg}")

    # ------------------------------------------------------------------ dump
    def dump(self) -> None:
        lg = logging.getLogger("summary")
        lg.info("📥 Download summary ▸ done=%d skip=%d error=%d total=%d",
                self.downloads["done"], self.downloads["skip"],
                self.downloads["error"], sum(self.downloads.values()))
        lg.info("📦 Staging summary ▸ done=%d error=%d total=%d",
                self.staging["done"], self.staging["error"],
                sum(self.staging.values()))
        lg.info("📊 SDE loading complete: %d loaded, %d errors",
                self.sde["done"], self.sde["error"])

        if self.errors:
            lg.info("🚨 First errors:")
            for line in self.errors:
                lg.info("    • %s", line)
