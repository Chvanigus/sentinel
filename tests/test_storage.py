"""Тесты безопасных файловых адаптеров processing."""

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


def test_tile_archive_copies_result_to_date_hierarchy(tmp_path):
    """Tile-level растр копируется в иерархию долговременного архива."""
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


def test_geometry_exporter_atomically_writes_geojson(tmp_path):
    """Экспортёр записывает валидный GeoJSON без временного остатка."""
    destination = tmp_path / "field.geojson"
    geometry = geojson.Point((37.0, 55.0))

    result = FieldGeometryExporter().export(geometry, destination)

    assert result == destination
    assert geojson.loads(destination.read_text(encoding="utf-8")) == geometry
    assert not destination.with_suffix(".geojson.tmp").exists()
