"""Класс для работы с растровыми изображениями."""
from osgeo import gdal
import os

from core.const import FORMAT_GEOTIFF
from core.logging import get_logger


class RastrProcessing:
    """
    Класс для работы с растровыми изображениями.
    """

    def __init__(
            self, src_path: str, dst_path: str, format_file: str = None,
            dst_srs: str = None
    ):
        self._src_path = src_path
        self._src_ds = self._get_dataset()
        self._dst_path = dst_path
        self.logger = get_logger()

        if format_file is not None:
            self._fm = format_file
        else:
            self._fm = FORMAT_GEOTIFF

        if dst_srs is not None:
            self._dst_srs = dst_srs
        else:
            self._dst_srs = "EPSG:32638"

    def _get_dataset(self) -> gdal.Dataset:
        """Возвращает gdal.Dataset переданного растрового изображения."""
        return gdal.Open(self._src_path)

    def _set_rastr_translate_options(self):
        """
        Устанавливает настройки для перепроецирования растрового изображения.
        """
        return gdal.TranslateOptions(
            format=self._fm, outputSRS=self._dst_srs,
            outputType=gdal.GDT_Int16
        )

    def clip_by_shp(
            self, mask_file_path: str, x_res: int = None,
            y_res: int = None
    ) -> None:
        """
        Обрезка tiff изображения по векторной маске
        :param mask_file_path: Маска, по которой происходит обрезка
        :param x_res: Размеры изображения с единицами измерения при геопривязке
        :param y_res: Размеры изображения с единицами измерения при геопривязке
        """
        if not x_res or not y_res:
            gt = self._src_ds.GetGeoTransform()
            x_res = abs(gt[1])
            y_res = abs(gt[5])

        srs = self._src_ds.GetProjection()

        gdal.SetConfigOption("GDALWARP_IGNORE_BAD_CUTLINE", "YES")

        if os.path.exists(self._dst_path):
            os.remove(self._dst_path)

        try:
            gdal.Warp(
                self._dst_path,
                self._src_ds,
                format=self._fm,
                cutlineDSName=mask_file_path,
                cropToCutline=True,
                xRes=x_res,
                yRes=y_res,
                dstSRS=srs,
                multithread=True,
                warpOptions=["CUTLINE_ALL_TOUCHED=TRUE"]
            )
        except RuntimeError as e:
            self.logger.error(
                f"Ошибка при обрезке изображения {self._src_path}: {e}"
            )
            pass

    def projection_raster(self, dst_path: str) -> None:
        """
        Перепроецирование входного файла в заданную проекцию.
        :param dst_path: Путь к выходному файлу
        """
        gdal.Translate(
            srcDS=self._src_ds,
            destName=dst_path,
            options=self._set_rastr_translate_options()
        )
