"""Пути рабочего пространства processing pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkspacePaths:
    """Явная конфигурация каталогов вместо скрытых global imports."""

    temporary: Path
    intermediate: Path
    processed: Path
    ndvi: Path


@dataclass(frozen=True)
class ProcessingOptions:
    """Численные параметры GIS-обработки."""

    destination_srid: int
    nodata: float
