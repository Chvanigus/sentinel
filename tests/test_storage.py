"""Тесты безопасных файловых адаптеров processing."""

import geojson
import pytest

from core.filesystem import clear_directory_contents
from processing.storage import FieldGeometryExporter


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


def test_geometry_exporter_atomically_writes_geojson(tmp_path):
    """Экспортёр записывает валидный GeoJSON без временного остатка."""
    destination = tmp_path / "field.geojson"
    geometry = geojson.Point((37.0, 55.0))

    result = FieldGeometryExporter().export(geometry, destination)

    assert result == destination
    assert geojson.loads(destination.read_text(encoding="utf-8")) == geometry
    assert not destination.with_suffix(".geojson.tmp").exists()
