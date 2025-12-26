"""Классы для работы с gdal.Dataset"""
import numpy as np
from osgeo import gdal

from core import settings
from core.const import FORMAT_GEOTIFF

gdal.UseExceptions()


class GDALDatasetContextManager:
    """Диспетчер контекста для открытия файла в виде gdal.Dataset."""
    def __init__(self, file_path, mode=gdal.GA_ReadOnly):
        self._file_path = file_path
        self._mode = mode
        self._dataset = None

    def __enter__(self):
        self._dataset = gdal.Open(self._file_path, self._mode)
        if self._dataset is None:
            raise FileNotFoundError(
                f"Такого файла не существует: {self._file_path}"
            )
        return self._dataset

    def __exit__(self, exc_type, exc_value, traceback):
        if self._dataset is not None:
            self._dataset = None


class GDALDatasetProcessing:
    """Класс для работы с gdal.Dataset."""

    def __init__(
            self, src_path: str, dst_path: str,
            np_array: np.ndarray = None, format_file: str = FORMAT_GEOTIFF,
            nodata: int = None, nband: int = None, data_type=None,
            x_size: int = None, y_size: int = None, band: int = None
    ) -> None:
        self._src_path = src_path
        self._src_ds = self._get_src_ds()
        self._band = 1 if not band else band
        self._array = np_array
        self._dst_path = dst_path
        self._format = format_file
        self._nodata = settings.NODATA if not nodata else nodata
        self._nband = self._set_nband() if not nband else nband
        self._xsize = self._set_xsize() if not x_size else x_size
        self._ysize = self._set_ysize() if not y_size else y_size
        self._data_type = self._set_data_type() if not data_type else data_type

    def _get_src_ds(self):
        with GDALDatasetContextManager(self._src_path) as ds:
            src_ds = ds
        return src_ds

    def _set_nband(self) -> int:
        return self._src_ds.RasterCount

    def _set_xsize(self) -> int:
        return self._src_ds.RasterXSize

    def _set_ysize(self) -> int:
        return self._src_ds.RasterYSize

    def _set_data_type(self):
        return self._src_ds.GetRasterBand(self._band).DataType

    def _get_driver(self, xsize: int = None, ysize: int = None):
        """Создает драйвер на основе gdal.Dataset."""
        driver = gdal.GetDriverByName(self._format)

        if not xsize:
            x_size = self._xsize
        else:
            x_size = xsize

        if not ysize:
            y_size = self._ysize
        else:
            y_size = ysize

        return driver.Create(
            self._dst_path, x_size, y_size, self._nband,
            gdal.GDT_Float32
        )

    def _set_transform_and_projection(self, driver):
        driver.SetGeoTransform(self._src_ds.GetGeoTransform())
        driver.SetProjection(self._src_ds.GetProjection())
        return driver

    def _create_output_dataset(self) -> gdal.Dataset:
        """
        Создает выходной gdal.Dataset на основе заданных параметров
        :returns: gdal.Dataset.
        """
        driver = self._get_driver()
        dst_ds = self._set_transform_and_projection(driver)

        for band in range(self._nband):
            band += 1
            if self._nodata:
                dst_ds.GetRasterBand(band).SetNoDataValue(self._nodata)

        return dst_ds

    def create_output_ds(self) -> gdal.Dataset:
        """
        Создает выходной gdal.Dataset на основе заданных параметров
        :returns: gdal.Dataset.
        """
        return self._create_output_dataset()

    def create_file_from_array(self):
        """
        Создание файла из массива пикселей.
        """
        y_size, x_size = self._array.shape
        driver = self._get_driver(ysize=y_size, xsize=x_size)
        driver.GetRasterBand(1).WriteArray(self._array)

        if self._nodata:
            driver.GetRasterBand(1).SetNoDataValue(self._nodata)

        driver = self._set_transform_and_projection(driver)
        driver.FlushCache()
        driver = None

    def save_ds_to_geotiff_file(self) -> None:
        """
        Сохраняет gdal.Dataset в новый tiff файл по указанному пути.
        """

        dst_ds = self._create_output_dataset()
        for band in range(self._nband):
            band += 1
            dst_ds.GetRasterBand(band).WriteArray(
                self._src_ds.GetRasterBand(band).ReadAsArray()
            )
        dst_ds = None
