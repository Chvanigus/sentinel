"""Класс обработки векторных изображений."""
import os

from osgeo import ogr, osr, gdal

from core import settings, utils, const


class VectorProcessing:
    """Класс обработки векторных изображений."""

    def __init__(
            self, dst_path: str = None, dst_srs: str = None,
            src_path: str = None, format_file: str = const.FORMAT_SHP
    ) -> None:
        self._src_path = src_path
        self._dst_path = dst_path
        self._fm = format_file

        if not dst_srs:
            self._dst_srs = osr.SpatialReference()
            self._dst_srs.ImportFromEPSG(settings.DESTSRID)
        else:
            self._dst_srs = dst_srs

    def _create_empty_shape_file(self) -> dict:
        """
        Создание shape файла (пустой, без заданной геометрии).
        :returns: Словарь с данными о shape файле -
                  {"ds": osr.DataSource, "layer": ogr.Layer, "path": String}
        """
        layer_name = utils.get_basename(self._dst_path)

        if os.path.exists(self._dst_path):
            os.remove(self._dst_path)

        driver = ogr.GetDriverByName(const.FORMAT_SHP)
        dst_ds = driver.CreateDataSource(self._dst_path)
        dst_layer = dst_ds.CreateLayer(layer_name, srs=self._dst_srs)

        return {"ds": dst_ds, "layer": dst_layer, "path": self._dst_path}

    def _set_vector_translate_options(self):
        """Возвращает настройки перепроецирования вектора."""
        return gdal.VectorTranslateOptions(
            format=self._fm, dstSRS=self._dst_srs
        )

    def _set_vector_rasterization_options(
            self, bounds: list, xRes: int = 20, yRes: int = 20,
            burn_values: int = 1,
    ):
        """Возвращает настройки растеризации вектора."""
        return gdal.RasterizeOptions(
            format=self._fm, outputSRS=self._dst_srs, xRes=xRes, yRes=yRes,
            burnValues=burn_values, outputType=gdal.GDT_Int16, noData=0,
            outputBounds=bounds
        )
