"""Тесты безопасных файловых адаптеров processing."""

import shutil
from datetime import date
from pathlib import Path

import geojson
import pytest

from core.filesystem import clear_directory_contents
from processing.domain import ProductLevel, SceneContext
from processing.storage import FieldGeometryExporter, GeowareTileArchive


def test_clear_directory_contents_preserves_root(tmp_path):
    """Очистка удаляет вложения, но сохраняет явно переданный корень."""
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "data.bin").write_bytes(b"data")
    (tmp_path / "root.txt").write_text("data", encoding="utf-8")

    clear_directory_contents(tmp_path)

    assert tmp_path.is_dir()
    assert list(tmp_path.iterdir()) == []


def test_clear_directory_contents_rejects_file(tmp_path):
    """Файл нельзя передать вместо каталога для очистки."""
    target = tmp_path / "file.txt"
    target.write_text("data", encoding="utf-8")

    with pytest.raises(NotADirectoryError):
        clear_directory_contents(target)


def test_tile_archive_stores_result_in_date_hierarchy(tmp_path):
    """Tile-level растр сохраняется в иерархию долговременного архива."""
    source = tmp_path / "source.tif"
    source.write_bytes(b"raster")
    scene = SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=ProductLevel.L2A,
        agroids=(1, 3, 4),
    )

    destination = GeowareTileArchive(tmp_path / "geoware").store(
        scene,
        source,
        "ndvi",
    )

    assert destination == (
        tmp_path / "geoware" / "2026" / "T38ULA" / "ndvi" / "07"
        / "source.tif"
    )
    assert destination.read_bytes() == b"raster"


def test_tile_archive_does_not_recopy_identical_result(tmp_path, monkeypatch):
    """Повторное архивирование идентичного файла не выполняет copy2."""
    source = tmp_path / "source.tif"
    source.write_bytes(b"raster")
    scene = SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=ProductLevel.L2A,
        agroids=(1, 3, 4),
    )
    archive = GeowareTileArchive(tmp_path / "geoware")
    destination = archive.store(scene, source, "ndvi")
    monkeypatch.setattr(
        "processing.storage.shutil.copy2",
        lambda *_args: pytest.fail("Идентичный файл не надо копировать"),
    )

    assert archive.store(scene, source, "ndvi") == destination


def test_tile_archive_falls_back_to_atomic_copy(tmp_path, monkeypatch):
    """Разные файловые разделы используют copy2 через временный файл."""
    source = tmp_path / "source.tif"
    source.write_bytes(b"raster")
    scene = SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=ProductLevel.L2A,
        agroids=(1, 3, 4),
    )
    copied_to = []
    original_copy = shutil.copy2
    monkeypatch.setattr(
        "processing.storage.os.link",
        lambda *_args: (_ for _ in ()).throw(OSError("cross-device")),
    )

    def recording_copy(source_path, destination_path):
        """Запоминает fallback-копирование и выполняет его."""
        copied_to.append(Path(destination_path))
        return original_copy(source_path, destination_path)

    monkeypatch.setattr("processing.storage.shutil.copy2", recording_copy)

    destination = GeowareTileArchive(tmp_path / "geoware").store(
        scene,
        source,
        "ndvi",
    )

    assert destination.read_bytes() == b"raster"
    assert copied_to == [destination.with_suffix(".tif.partial")]
    assert not copied_to[0].exists()


def test_tile_archive_restores_cached_product(tmp_path):
    """Повторный запуск восстанавливает готовый tile-level растр без GDAL."""
    source = tmp_path / "source.tif"
    source.write_bytes(b"cached-raster")
    scene = SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=ProductLevel.L2A,
        agroids=(1, 3, 4),
    )
    archive = GeowareTileArchive(tmp_path / "geoware")
    archive.store(scene, source, "ndvi")
    source.unlink()

    assert archive.restore(scene, source, "ndvi") is True
    assert source.read_bytes() == b"cached-raster"
    assert archive.restore(
        scene,
        tmp_path / "missing.tif",
        "ndvi",
    ) is False


def test_geometry_exporter_atomically_writes_geojson(tmp_path):
    """Экспортёр записывает валидный GeoJSON без временного остатка."""
    destination = tmp_path / "field.geojson"
    geometry = geojson.Point((37.0, 55.0))

    result = FieldGeometryExporter().export(geometry, destination)

    assert result == destination
    assert geojson.loads(destination.read_text(encoding="utf-8")) == geometry
    assert not destination.with_suffix(".geojson.tmp").exists()
