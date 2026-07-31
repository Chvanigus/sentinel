"""Тесты выбора GDAL-алгоритмов при вырезке продуктов Sentinel."""

from contextlib import nullcontext
from datetime import date
from types import SimpleNamespace

from processing.processors import sentinel
from processing.processors.sentinel import AgroCropProcessor


class SourceDataset:
    """Минимальный исходный растр."""

    def GetProjection(self):
        """Возвращает тестовую проекцию."""
        return "SOURCE_WKT"

    def GetGeoTransform(self):
        """Возвращает разрешение 10 метров."""
        return 0.0, 10.0, 0.0, 20.0, 0.0, -10.0


class SpatialReference:
    """Минимальная пространственная ссылка."""

    def __init__(self, wkt=None):
        self.wkt = wkt

    def ImportFromEPSG(self, srid):
        """Сохраняет выбранный SRID."""
        self.srid = srid


class WarpResult:
    """Имитирует успешный GDAL Warp."""

    def FlushCache(self):
        """Имитирует сброс буфера."""


def test_crop_uses_nearest_for_scl_and_lanczos_for_continuous_data(
        monkeypatch,
):
    """Категории SCL не получают несуществующие интерполированные классы."""
    calls = []

    def warp(*_args, **options):
        """Запоминает выбранный алгоритм."""
        calls.append(options)
        return WarpResult()

    monkeypatch.setattr(
        sentinel,
        "gdal",
        SimpleNamespace(
            GRA_NearestNeighbour="nearest",
            GRA_Lanczos="lanczos",
            Warp=warp,
        ),
    )
    monkeypatch.setattr(
        sentinel,
        "osr",
        SimpleNamespace(SpatialReference=SpatialReference),
    )
    monkeypatch.setattr(
        sentinel,
        "open_raster",
        lambda _path: nullcontext(SourceDataset()),
    )
    monkeypatch.setattr(
        sentinel,
        "atomic_raster_path",
        lambda _path: nullcontext("result.tif.partial"),
    )
    processor = AgroCropProcessor(
        SimpleNamespace(acquired_on=date(2026, 7, 1)),
        paths=object(),
        field_data=object(),
        options=SimpleNamespace(
            destination_srid=3857,
            nodata=-9999.0,
        ),
    )
    monkeypatch.setattr(
        processor,
        "_get_bounds",
        lambda *_args: (0.0, 0.0, 10.0, 10.0),
    )

    processor._warp("scl.tif", "scl-crop.tif", 3, "scl")
    processor._warp("ndvi.tif", "ndvi-crop.tif", 3, "ndvi")

    assert [call["resampleAlg"] for call in calls] == [
        "nearest",
        "lanczos",
    ]
