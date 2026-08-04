"""Файловые адаптеры обработки геометрий."""
from __future__ import annotations

from pathlib import Path

import geojson


class FieldGeometryExporter:
    """Сериализует полученную из repository геометрию поля."""

    def export(self, geometry, destination: str | Path) -> Path:
        """Атомарно записывает геометрию поля в GeoJSON."""
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                geojson.dump(geometry, stream)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
        return path
