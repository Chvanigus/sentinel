"""Тесты пересечения границ хозяйства и растра."""

from contextlib import nullcontext
from types import SimpleNamespace

from processing import geometry


class RasterDataset:
    """Минимальный dataset с геопривязкой квадратного растра."""

    RasterXSize = 4
    RasterYSize = 4

    def GetProjection(self):
        """Возвращает условную исходную систему координат."""
        return "SOURCE_WKT"

    def GetGeoTransform(self):
        """Возвращает сетку 10 метров с верхней границей 40."""
        return 0.0, 10.0, 0.0, 40.0, 0.0, -10.0


class SpatialReference:
    """Минимальная пространственная ссылка для теста."""

    imported_epsg = None

    def __init__(self, wkt=None):
        """Сохраняет переданный WKT."""
        self.wkt = wkt

    def ImportFromEPSG(self, srid):
        """Фиксирует выбранный целевой SRID."""
        SpatialReference.imported_epsg = srid


class IdentityTransformer:
    """Оставляет координаты без изменения."""

    def TransformPoints(self, points):
        """Добавляет третью координату к переданным точкам."""
        return [(x, y, 0.0) for x, y in points]


def test_intersection_uses_raster_extent_and_target_srid(monkeypatch):
    """Функция ограничивает bounds экстентом растра и настраивает SRID."""
    monkeypatch.setattr(
        geometry,
        "open_raster",
        lambda _source: nullcontext(RasterDataset()),
    )
    monkeypatch.setattr(
        geometry,
        "osr",
        SimpleNamespace(
            SpatialReference=SpatialReference,
            CoordinateTransformation=lambda *_args: IdentityTransformer(),
        ),
    )
    SpatialReference.imported_epsg = None

    result = geometry.intersect_raster_bounds(
        (5.0, 5.0, 25.0, 50.0),
        "source.tif",
        3857,
    )

    assert result == (5.0, 5.0, 25.0, 40.0)
    assert SpatialReference.imported_epsg == 3857
