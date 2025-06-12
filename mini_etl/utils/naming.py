from __future__ import annotations


def sanitize(name: str) -> str:
    return name.lower().replace(" ", "_")
