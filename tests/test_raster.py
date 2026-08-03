"""Тесты GDAL-операций без реального файлового драйвера."""

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from processing import raster


class SourceDataset:
    """Минимальный исходный dataset."""

    RasterXSize = 2
    RasterYSize = 1

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

    RasterXSize = 2
    RasterYSize = 1

    def ReadAsArray(self):
        """Возвращает тестовые значения."""
        return np.array([[0.4, -9999.0]], dtype=np.float64)

    def GetGeoTransform(self):
        """Возвращает геотрансформацию вырезки."""
        return 0.0, 10.0, 0.0, 10.0, 0.0, -10.0

    def GetProjection(self):
        """Возвращает проекцию вырезки."""
        return "EPSG:3857"


class MultiBandMemoryResult(MemoryResult):
    """Имитирует совместную двухканальную вырезку NDVI/SCL."""

    def ReadAsArray(self):
        """Возвращает NDVI и SCL одним массивом."""
        return np.array(
            [
                [[0.4, -9999.0]],
                [[4.0, 0.0]],
            ],
            dtype=np.float64,
        )


class CoverageBand:
    """Имитирует растровую маску геометрии поля."""

    def Fill(self, _value):
        """Имитирует начальное заполнение маски."""

    def ReadAsArray(self):
        """Возвращает один пиксель поля и один пиксель рамки."""
        return np.array([[1, 0]], dtype=np.uint8)


class CoverageDataset:
    """Имитирует MEM dataset растеризованного полигона."""

    def SetGeoTransform(self, _transform):
        """Принимает геотрансформацию."""

    def SetProjection(self, _projection):
        """Принимает проекцию."""

    def GetRasterBand(self, _number):
        """Возвращает маску покрытия."""
        return CoverageBand()


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


def test_field_reader_uses_memory_and_explicit_nodata(monkeypatch):
    """Reader не пишет TIFF и сохраняет nodata вне полигона."""
    captured = {}

    def warp(destination, source, **options):
        """Запоминает параметры in-memory GDAL Warp."""
        captured.update(
            destination=destination,
            source=source,
            options=options,
        )
        return MemoryResult()

    class Vector:
        """Имитирует GeoJSON dataset."""

        def GetLayer(self):
            """Возвращает условный векторный слой."""
            return "field-layer"

    class Driver:
        """Создаёт MEM dataset покрытия."""

        def Create(self, *_args):
            """Возвращает тестовый растр покрытия."""
            return CoverageDataset()

    monkeypatch.setattr(
        raster,
        "gdal",
        SimpleNamespace(
            Warp=warp,
            OpenEx=lambda *_args: Vector(),
            GetDriverByName=lambda _name: Driver(),
            RasterizeLayer=lambda *_args, **_kwargs: 0,
            Unlink=lambda _path: None,
            OF_VECTOR=1,
            GDT_Byte=1,
        ),
    )
    source = SourceDataset()
    monkeypatch.setattr(
        raster,
        "open_raster",
        lambda _path: nullcontext(source),
    )

    with raster.FieldRasterReader(
            "ndvi.tif",
            nodata=-9999.0,
    ) as reader:
        result = reader.clip("field.geojson")

    assert result.values.dtype == np.float32
    np.testing.assert_allclose(result.values, [[0.4, -9999.0]])
    np.testing.assert_array_equal(result.coverage, [[True, False]])
    assert result.scl is None
    assert captured["destination"] == ""
    assert captured["source"] is source
    assert captured["options"]["format"] == "MEM"
    assert captured["options"]["srcNodata"] == -9999.0
    assert captured["options"]["dstNodata"] == -9999.0
    assert "INIT_DEST=NO_DATA" in captured["options"]["warpOptions"]


def test_field_reader_combines_ndvi_and_scl_in_one_warp(monkeypatch):
    """NDVI и SCL открываются один раз и вырезаются одним Warp."""

    class Vector:
        """Имитирует GeoJSON dataset."""

        def GetLayer(self):
            """Возвращает условный векторный слой."""
            return "field-layer"

    class Driver:
        """Создаёт MEM dataset покрытия."""

        def Create(self, *_args):
            """Возвращает тестовый растр покрытия."""
            return CoverageDataset()

    captured = {"warps": 0, "builds": 0}

    def build_vrt(path, sources, **options):
        """Фиксирует создание одного двухканального VRT."""
        captured["builds"] += 1
        captured["vrt"] = (path, sources, options)
        return SourceDataset()

    def warp(*_args, **options):
        """Фиксирует единственный Warp обоих каналов."""
        captured["warps"] += 1
        captured["warp_options"] = options
        return MultiBandMemoryResult()

    fake_gdal = SimpleNamespace(
        BuildVRT=build_vrt,
        Warp=warp,
        OpenEx=lambda *_args: Vector(),
        GetDriverByName=lambda _name: Driver(),
        RasterizeLayer=lambda *_args, **_kwargs: 0,
        Unlink=lambda _path: None,
        OF_VECTOR=1,
        GDT_Byte=1,
    )
    monkeypatch.setattr(raster, "gdal", fake_gdal)
    sources = {
        "ndvi.tif": SourceDataset(),
        "scl.tif": SourceDataset(),
    }
    monkeypatch.setattr(
        raster,
        "open_raster",
        lambda path: nullcontext(sources[str(path)]),
    )

    with raster.FieldRasterReader(
            "ndvi.tif",
            scl_path="scl.tif",
            nodata=-9999.0,
    ) as reader:
        result = reader.clip("field.geojson")

    np.testing.assert_allclose(result.values, [[0.4, -9999.0]])
    np.testing.assert_array_equal(result.coverage, [[True, False]])
    np.testing.assert_allclose(result.scl, [[4.0, 0.0]])
    assert captured["builds"] == 1
    assert captured["warps"] == 1
    assert captured["vrt"][2] == {"separate": True}
    assert captured["warp_options"]["srcNodata"] == [-9999.0, 0]
    assert captured["warp_options"]["dstNodata"] == [-9999.0, 0]
