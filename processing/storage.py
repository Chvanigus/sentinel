"""Файловые адаптеры сохранения результатов processing."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import geojson

from core.logging import get_logger
from processing.domain import SceneContext


class GeowareTileArchive:
    """Архивирует tile-level результат без лишнего копирования на одном диске."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.logger = get_logger(self.__class__.__name__)

    def store(
            self,
            scene: SceneContext,
            source: str | Path,
            product: str,
    ) -> Path:
        """Атомарно связывает или копирует растр в долговременную иерархию."""
        source_path = Path(source)
        if not source_path.is_file():
            raise FileNotFoundError(source_path)
        destination_dir = (
            self.root
            / str(scene.acquired_on.year)
            / scene.tile.upper()
            / product.lower()
            / f"{scene.acquired_on.month:02d}"
        )
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / source_path.name
        if destination.exists():
            source_stat = source_path.stat()
            destination_stat = destination.stat()
            if (
                    source_stat.st_size == destination_stat.st_size
                    and source_stat.st_mtime_ns
                    == destination_stat.st_mtime_ns
            ):
                self.logger.info(
                    "Результат уже находится в geoware: %s",
                    destination,
                )
                return destination

        temporary = destination.with_suffix(destination.suffix + ".partial")
        temporary.unlink(missing_ok=True)
        linked = False
        try:
            try:
                os.link(source_path, temporary)
                linked = True
            except OSError:
                shutil.copy2(source_path, temporary)
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)

        operation = "hardlink" if linked else "copy"
        self.logger.info(
            "Результат сохранён в geoware (%s): %s",
            operation,
            destination,
        )
        return destination


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
