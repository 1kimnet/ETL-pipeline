# GitHub Copilot – codeGeneration.instructions

## Scope and Dependencies

* Use **only** libraries that ship with **ArcGIS Pro 3.3** or **ArcGIS Server 11.3** (`arcpy`, standard library).
* If—and  *only if* —extra libraries are indispensable, pick ones that can be trivially vendored (single‑file or pure‑Python wheels).

## File‑Creation Policy

* **Never create new files unless the user explicitly asks for them.**

## Architectural Guidelines

* Follow  **Clean Architecture** —organize code into clearly segmented packages:
  * `utils/` – lightweight helpers and adapters
  * `models/` – `@dataclass` domain entities & configurations
  * `handlers/` – action‑oriented classes/functions that orchestrate work
  * `pipeline/` – high‑level workflows, CLI entry points, notebooks, etc.
* Rely on a **handler‑based approach** for extensibility and testability.

## Coding Conventions

1. **Whole Functions Only** – suggest entire functions, not fragments.
2. **Elegant Focus** – address the specific task without superfluous checks or nested `if`‑trees; use guard clauses instead.
3. **Modern Python** – assume 3.11; use `pathlib`, `contextlib`, `typing.Final`, pattern matching where clear.
4. **Type Hints Everywhere** – 100 % static typing, `|` unions, generics.
5. **Dataclasses** – `@dataclass(slots=True, frozen=True)` for configs & DTOs.
6. **Readable Logging** – emojis for quick scanning:
   * 🔄 start / progress
   * ✅ success
   * ⚠️ warning
   * ❌ error
7. **Max line length 88** , PEP 8 compliant, ruff & black friendly.

## Sample Layout

```text
project_root/
├── handlers/
│   └── process_raster.py
├── models/
│   └── raster_config.py
├── utils/
│   └── io.py
├── pipeline/
│   └── main.py
└── tests/
    └── test_process_raster.py
```

## Function Template

```python
from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

import arcpy  # ArcGIS Pro 3.3 builtin

from models.raster_config import RasterConfig

log: Final = logging.getLogger(__name__)


def process_raster(input_path: Path, cfg: RasterConfig) -> arcpy.Raster:
    """🔄 Process a raster and return the result.

    Args:
        input_path: Path to the input raster.
        cfg: Immutable processing parameters.

    Returns:
        The processed raster object.
    """
    # --- validation -------------------------------------------------------
    if not input_path.exists():
        log.error("❌ Raster not found: %s", input_path)
        raise FileNotFoundError(input_path)

    # --- processing -------------------------------------------------------
    with arcpy.EnvManager(overwriteOutput=True):
        # concise, single‑purpose processing steps here
        result: arcpy.Raster = arcpy.sa.Slope(str(input_path))  # type: ignore[attr-defined]

    log.info("✅ Raster processing complete → %s", result)
    return result
```

## Exception Handling

* Catch **specific** exceptions; re‑raise with context if needed.
* Use `contextlib.suppress()` where benign.

## Review Checklist (for every suggestion)

* [ ] Only ArcGIS/stdlib deps used
* [ ] Entire functions provided
* [ ] Type‑annotated & dataclass‑driven
* [ ] Emoji logging included
* [ ] Fits clean architecture structure
* [ ] Concise and readable
