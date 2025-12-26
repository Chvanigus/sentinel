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

    def create_shape_file_from_wkt(self, wkt_geometry: str):
        """
        Создает shape файл из WKT геометрии.
        :param wkt_geometry: WKT геометрия
        :returns: Путь к выходному файлу
        """
        data = self._create_empty_shape_file()
        layer = data["layer"]

        geometry = ogr.CreateGeometryFromWkt(wkt_geometry)
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetGeometryDirectly(geometry)
        layer.CreateFeature(feature)
        feature.Destroy()

        data["ds"] = None

        return data["path"]

    def _set_vector_translate_options(self):
        """Возвращает настройки перепроецирования вектора."""
        return gdal.VectorTranslateOptions(
            format=self._fm, dstSRS=self._dst_srs
        )

    def projection_vector(self) -> None:
        """
        Перепроецирование входного файла в заданную проекцию.
        Сохраняет по указанному при инициализации класса пути.
        """
        gdal.VectorTranslate(
            destNameOrDestDS=self._dst_path,
            srcDS=self._src_path,
            options=self._set_vector_translate_options()
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

    def rasterization_vector(
            self, bounds: list, xRes: int = 20, yRes: int = 20,
            burn_values: int = 1,
    ):
        """
        Растеризация вектора.
        :param bounds: Границы по которым будет проходить растеризация.
        :param xRes: Разрешение пикселей на метр по X координате.
        :param yRes: Разрешение пикселей на метр по Y координате.
        :param burn_values: Хз.
        """
        gdal.Rasterize(
            destNameOrDestDS=self._dst_path,
            srcDS=self._src_path,
            options=self._set_vector_rasterization_options(
                bounds=bounds, xRes=xRes, yRes=yRes, burn_values=burn_values
            )
        )
