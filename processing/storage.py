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

    def _archive_path(
            self,
            scene: SceneContext,
            filename: str,
            product: str,
    ) -> Path:
        """Строит путь долговременной tile-level копии продукта."""
        return (
            self.root
            / str(scene.acquired_on.year)
            / scene.tile.upper()
            / product.lower()
            / f"{scene.acquired_on.month:02d}"
            / filename
        )

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
        destination = self._archive_path(
            scene,
            source_path.name,
            product,
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
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

    def restore(
            self,
            scene: SceneContext,
            destination: str | Path,
            product: str,
    ) -> bool:
        """Восстанавливает tile-level продукт из архива без повторного GDAL."""
        destination_path = Path(destination)
        if destination_path.is_file():
            return True
        source = self._archive_path(
            scene,
            destination_path.name,
            product,
        )
        if not source.is_file():
            return False

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination_path.with_suffix(
            destination_path.suffix + ".partial"
        )
        temporary.unlink(missing_ok=True)
        linked = False
        try:
            try:
                os.link(source, temporary)
                linked = True
            except OSError:
                shutil.copy2(source, temporary)
            temporary.replace(destination_path)
        finally:
            temporary.unlink(missing_ok=True)
        self.logger.info(
            "Tile-level продукт восстановлен (%s): %s",
            "hardlink" if linked else "copy",
            destination_path,
        )
        return True


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
