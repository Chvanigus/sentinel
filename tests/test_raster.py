"""Тесты GDAL-операций без реального файлового драйвера."""

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from processing import raster


class SourceDataset:
    """Минимальный исходный dataset."""

    def GetGeoTransform(self):
        """Возвращает разрешение 10 метров."""
        return 0.0, 10.0, 0.0, 20.0, 0.0, -10.0

    def GetProjection(self):
        """Возвращает тестовую проекцию."""
        return "EPSG:3857"


class TranslateResult:
    """Имитирует успешно созданный GDAL-растр."""

    def FlushCache(self):
        """Имитирует сброс буфера."""


class MemoryResult:
    """Имитирует вырезанный в памяти растр."""

    def ReadAsArray(self):
        """Возвращает тестовые значения."""
        return np.array([[0.4, -9999.0]], dtype=np.float64)


def test_translate_preserves_source_type_and_writes_atomically(
        tmp_path,
        monkeypatch,
):
    """Translate не раздувает Byte-источник принудительным Int16."""
    captured = {}

    def translate_options(**options):
        """Запоминает параметры GDAL Translate."""
        captured["options"] = options
        return options

    def translate(**parameters):
        """Создаёт временный файл так же, как GDAL driver."""
        captured["translate"] = parameters
        Path(parameters["destName"]).write_bytes(b"raster")
        return TranslateResult()

    monkeypatch.setattr(
        raster,
        "gdal",
        SimpleNamespace(
            TranslateOptions=translate_options,
            Translate=translate,
        ),
    )
    monkeypatch.setattr(
        raster,
        "open_raster",
        lambda _path: nullcontext(SourceDataset()),
    )
    destination = tmp_path / "result.tif"

    raster.translate_to_geotiff("source.jp2", destination)

    assert destination.read_bytes() == b"raster"
    assert "outputType" not in captured["options"]
    assert "TILED=YES" in captured["options"]["creationOptions"]
    assert "NUM_THREADS=ALL_CPUS" in captured["options"]["creationOptions"]
    assert captured["translate"]["destName"].endswith(".tif.partial")


def test_clip_by_mask_array_uses_memory_and_explicit_nodata(monkeypatch):
    """Полевой clip не пишет TIFF и сохраняет nodata вне полигона."""
    captured = {}

    def warp(destination, source, **options):
        """Запоминает параметры in-memory GDAL Warp."""
        captured.update(
            destination=destination,
            source=source,
            options=options,
        )
        return MemoryResult()

    monkeypatch.setattr(raster, "gdal", SimpleNamespace(Warp=warp))
    source = SourceDataset()
    monkeypatch.setattr(
        raster,
        "open_raster",
        lambda _path: nullcontext(source),
    )

    result = raster.clip_by_mask_array(
        "ndvi.tif",
        "field.geojson",
        nodata=-9999.0,
    )

    assert result.dtype == np.float32
    np.testing.assert_allclose(result, [[0.4, -9999.0]])
    assert captured["destination"] == ""
    assert captured["source"] is source
    assert captured["options"]["format"] == "MEM"
    assert captured["options"]["srcNodata"] == -9999.0
    assert captured["options"]["dstNodata"] == -9999.0
    assert "INIT_DEST=NO_DATA" in captured["options"]["warpOptions"]
