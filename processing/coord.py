"""Класс для работы с координатами."""
import functools
from typing import Tuple, List

from osgeo import gdal, osr

from core import settings


class CoordProcessing:
    """
    Работа с геопривязанным растром: определение пересечения растровых
    границ и целевых координатных ограничений.
    """

    def __init__(self, bounds: Tuple[float, float, float, float],
                 src_path: str):
        self._bounds = bounds
        self._src_path = src_path
        self._src_ds = self._open_dataset()
        self._src_srs = osr.SpatialReference(wkt=self._src_ds.GetProjection())

    def _open_dataset(self) -> gdal.Dataset:
        """
        Открывает растровый файл через GDAL.
        """
        ds = gdal.Open(self._src_path)
        if ds is None:
            raise FileNotFoundError(
                f"Не удалось открыть файл {self._src_path}"
            )
        return ds

    @functools.cached_property
    def dst_srs(self) -> osr.SpatialReference:
        """
        Возвращает объект пространственной ссылки целевой СК.
        """
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(settings.DESTSRID)
        return srs

    @functools.cached_property
    def transformer(self) -> osr.CoordinateTransformation:
        """
        Координатный трансформер из исходной СК в целевую.
        Возвращает объект для преобразования координат.
        """
        return osr.CoordinateTransformation(self._src_srs, self.dst_srs)

    @functools.cached_property
    def geotransform(self) -> Tuple[float, float, float, float, float, float]:
        """
        Параметры геотрансформации GDAL для текущего датасета.

        Формат возвращаемого кортежа:
            (originX, pixelWidth, rotationX, originY, rotationY, pixelHeight)

        Возвращает параметры аффинного преобразования.
        """
        return self._src_ds.GetGeoTransform()

    @functools.cached_property
    def raster_size(self) -> Tuple[int, int]:
        """
        Возвращает число столбцов (RasterXSize), число строк (RasterYSize).
        """
        return self._src_ds.RasterXSize, self._src_ds.RasterYSize

    def _get_pixel_corners(self) -> List[Tuple[int, int]]:
        """
        Возвращает координаты пиксельных углов растрового изображения.

        Перечисляет четыре угла: (0,0), (width,0), (width,height), (0,height).
        """
        w, h = self.raster_size
        return [(0, 0), (w, 0), (w, h), (0, h)]

    def find_band_bounds(self) -> List[float]:
        """
        Возвращает [xmin, ymin, xmax, ymax] —
        пересечение исходных bounds и экстента растра.

        В душе не знаю, зачем это нужно, но без этого не работает.
        @TODO: Понять, зачем этот метод нужен
        """
        # Вычисляем мировые координаты четырёх углов
        gt0, gt1, gt2, gt3, gt4, gt5 = self.geotransform
        corners = [
            (gt0 + px * gt1 + py * gt2, gt3 + px * gt4 + py * gt5)
            for px, py in self._get_pixel_corners()
        ]

        # Трансформируем все сразу
        pts = self.transformer.TransformPoints(corners)
        xs, ys = zip(*[(pt[0], pt[1]) for pt in pts])

        # Пересекаем с self._bounds
        xmin = max(self._bounds[0], min(xs))
        ymin = max(self._bounds[1], min(ys))
        xmax = min(self._bounds[2], max(xs))
        ymax = min(self._bounds[3], max(ys))

        return [xmin, ymin, xmax, ymax]
