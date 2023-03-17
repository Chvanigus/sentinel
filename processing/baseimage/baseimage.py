# -*- coding: utf-8 -*-

__author__ = 'Vladimir Salnikov, Maxim Ilmenskiy'
__date__ = 'May 2021'

import logging
import sys
from os import remove
from os.path import exists

import numpy as np
from osgeo import gdal, ogr, osr

import settings
import utils

logger = logging.getLogger('__name__')
gdal.UseExceptions()

FORMAT_MEM = 'MEM'
FORMAT_GEOTIFF = 'GTiff'
FORMAT_SHP = 'ESRI Shapefile'
FORMAT_GEOJSON = 'GEOJSON'


class OpenImage:
    """ Диспетчер контекста для открытия растровых файлов в виде gdal.Dataset"""

    def __init__(self, *args: str, ga_flag: int = gdal.GA_ReadOnly) -> None:
        """ Можно передать любое количество файлов

        :param ga_flag:
            Флаг чтения/записи или просто чтения данных из открываемого файла. По умолчанию - только чтение
        :param args:
            Пути или путь к tiff файлам
        """

        self.ga_flag = ga_flag
        self.tiff_files = args
        self.list_src_ds = []

    def __enter__(self) -> gdal.Dataset or list:
        """ Открывает все файлы, переданные в диспетчер

        :return:
            Список gdal.Dataset, если входных файлов больше одного или отдельно gdal.Dataset,
            если входной файл один
        """

        for arg in self.tiff_files:
            self.src_ds = gdal.Open(arg, self.ga_flag)
            self.list_src_ds.append(self.src_ds)
        if len(self.list_src_ds) == 1:
            return self.list_src_ds[0]
        else:
            return self.list_src_ds

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.list_src_ds = None
        self.src_ds = None
        self.tiff_files = None


def save_ds_to_file(src_ds: gdal.Dataset, dst_path: str, nodata: int or float = None,
                    data_type: int = None) -> None:
    """ Сохраняет gdal.Dataset в новый tiff файл

    :param src_ds:
        Входной gdal.Dataset
    :param dst_path:
        Путь к выходному файлу
    :param nodata:
        Значение пикселей, которые будут заменены на Nodata. (По умолчанию - None)
    :param data_type:
        Тип данных GDAL
    """
    try:
        driver = gdal.GetDriverByName(FORMAT_GEOTIFF)
        if not data_type:
            data_type = src_ds.GetRasterBand(1).DataType
        dst_ds = driver.Create(dst_path, src_ds.RasterXSize, src_ds.RasterYSize, src_ds.RasterCount, data_type)
        dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
        dst_ds.SetProjection(src_ds.GetProjection())

        for band in range(src_ds.RasterCount):
            band += 1
            if nodata is not None:
                dst_ds.GetRasterBand(band).SetNoDataValue(nodata)
            dst_ds.GetRasterBand(band).WriteArray(src_ds.GetRasterBand(band).ReadAsArray())
    except RuntimeError as e:
        logger.critical(f'Невозможно сохранить {dst_path}: {e}')
        sys.exit(-1)
    finally:
        dst_ds = None


def create_output_ds(src_ds: gdal.Dataset, dst_path: str, format_file: str,
                     data_type: int, nodata: int or float = None, nband: int = None) -> gdal.Dataset:
    """ Создает выходной gdal.Dataset на основе заданных параметров

    :param src_ds:
        Входной gdal.Dataset
    :param dst_path:
        Путь к результирующему файлу
    :param format_file:
        Выходной формат файла
    :param nband:
        Количество строк в выходном файле
    :param data_type:
        Тип данных GDAL
    :param nodata:
        Значение пикселей, которые будут заменены на Nodata. (По умолчанию - None)
    :returns:
        gdal.Dataset
    """
    try:
        if not nband:
            nband = src_ds.RasterCount
        driver = gdal.GetDriverByName(format_file)
        dst_ds = driver.Create(dst_path, src_ds.RasterXSize, src_ds.RasterYSize, nband, data_type)
        dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
        dst_ds.SetProjection(src_ds.GetProjection())
        for band in range(nband):
            band += 1
            if nodata:
                dst_ds.GetRasterBand(band).SetNoDataValue(nodata)

        return dst_ds

    except RuntimeError as e:
        logger.critical(f'Невозможно создать выходной dataset: {e}')
        sys.exit(-1)


def create_shape_file(dst_path: str, dst_srs: osr.SpatialReference = None) -> dict:
    """ Создание shape файла (пустой, без заданной геометрии).

    :param dst_path:
        Путь к выходному файлу
    :param dst_srs:
        Необходимая выходная проекция (SRS - Spatial reference system).
    :returns:
        Словарь с данными о shape файле - {'ds': osr.DataSource, 'layer': ogr.Layer, 'path': String}
    """
    try:
        if not dst_srs:
            dst_srs = osr.SpatialReference()
            dst_srs.ImportFromEPSG(settings.DESTSRID)

        layer_name = utils.get_filename(dst_path)
        if exists(dst_path):
            remove(dst_path)
        driver = ogr.GetDriverByName(FORMAT_SHP)
        dst_ds = driver.CreateDataSource(dst_path)
        dst_layer = dst_ds.CreateLayer(layer_name, srs=dst_srs)

        return {'ds': dst_ds, 'layer': dst_layer, 'path': dst_path}
    except RuntimeError as e:
        logger.critical(f'Невозможно создать пустой shape файл: {e}')
        sys.exit(-1)


def create_shape_file_from_wkt(wkt_geometry: str, dst_path: str):
    """ Создает shape файл из WKT геометрии.

    :param wkt_geometry:
        WKT геометрия (WKT - well-known text)
    :param dst_path:
        Путь к выходному файлу
    :returns:
        Путь к выходному файлу
    """
    try:
        field_layer = create_shape_file(dst_path=dst_path)

        geometry = ogr.CreateGeometryFromWkt(wkt_geometry)
        feature = ogr.Feature(field_layer['layer'].GetLayerDefn())
        feature.SetGeometryDirectly(geometry)
        field_layer['layer'].CreateFeature(feature)
        feature.Destroy()

        field_layer['ds'] = None

        return field_layer['path']

    except RuntimeError as e:
        logger.critical(f'Невозможно создать shape файл из WKT {wkt_geometry}: {e}')
        sys.exit(-1)


def clip_by_shp(src_path: str, mask_file_path: str, dst_path: str, flag_pass: bool = False,
                x_res: int = None, y_res: int = None):
    """ Обрезка tiff изображения по векторной маске

    :param src_path:
        Путь к входному файлу
    :param mask_file_path:
        Маска, по которой происходит обрезка
    :param dst_path:
        Путь к выходному файлу
    :param flag_pass:
        Флаг пропуска обработки изображения (в случае ошибки)
    :param x_res:
        Размеры изображения с единицами измерения при геопривязке. (По умолчанию - None)
    :param y_res:
        Размеры изображения с единицами измерения при геопривязке. (По умолчанию - None)
    """
    with OpenImage(src_path) as src_ds:
        try:
            if not (x_res or y_res):
                x_res = src_ds.GetGeoTransform()[1]
                y_res = src_ds.GetGeoTransform()[1]

            gdal.SetConfigOption('GDALWARP_IGNORE_BAD_CUTLINE', 'YES')

            gdal.Warp(dst_path, src_ds, format=FORMAT_GEOTIFF, xRes=x_res, yRes=y_res,
                      cutlineDSName=mask_file_path, cropToCutline=True)
        except RuntimeError as e:
            logger.critical(f'Невозможно обрезать изображение по маске. {e}')
            if flag_pass:
                pass
            else:
                sys.exit(-1)


# @TODO Честно я не разбирался с этой функцией, но без нее ничего не работает.
# @TODO Если я все правильно понял, данная функция переписывает позиции охвата геометрии в другую систему координат
# @TODO Проще говоря, без этой функции снимки не будут полноценно отображаться, поскольку  их СК будет сбита
def find_band_bounds(bounds, gt, xsize, ysize, src_srs, dst_srs):
    """ А"""
    coords = []
    xarr = [0, xsize]
    yarr = [0, ysize]

    for px in xarr:
        for py in yarr:
            x = gt[0] + (px * gt[1]) + (py * gt[2])
            y = gt[3] + (px * gt[4]) + (py * gt[5])
            coords.append([x, y])
        yarr.reverse()

    x_coords, y_coords = [], []
    transform = osr.CoordinateTransformation(src_srs, dst_srs)
    for x, y in coords:
        x, y, z = transform.TransformPoint(x, y)
        x_coords.append(x)
        y_coords.append(y)

    band_extent = [min(x_coords), min(y_coords), max(x_coords), max(y_coords)]

    new_bounds = [max(bounds[0], band_extent[0]), max(bounds[1], band_extent[1]), min(bounds[2], band_extent[2]),
                  min(bounds[3], band_extent[3])]

    return new_bounds


def projection_raster(src_path: str, dst_path: str,
                      dst_srs: str, format_file: str = FORMAT_GEOTIFF) -> None:
    """ Перепроецирование входного файла в заданную проекцию (только растр).

        :param src_path:
            Путь к входному файлу
        :param dst_path:
            Путь к выходному файлу
        :param dst_srs:
            Необходимая выходная проекция (SRS - Spatial reference system)
        :param format_file:
            Необходимый формат файла. По умолчанию 'GTiff'
    """

    with OpenImage(src_path) as src_ds:
        gt = gdal.TranslateOptions(format=format_file, outputSRS=dst_srs, outputType=gdal.GDT_Int16)
        try:
            gdal.Translate(srcDS=src_ds, destName=dst_path, options=gt)
        except RuntimeError or gdal.Error() as e:
            logger.critical('Невозможно перепроецировать изображение. Ошибка: {}'.format(e))
            sys.exit(-1)


def projection_vector(src_path: str, dst_path: str, dst_srs: str = None,
                      format_file: str = FORMAT_SHP) -> None:
    """ Перепроецирование входного файла в заданную проекцию (только вектор).

    :param src_path:
        Путь к входному файлу
    :param dst_path:
        Путь к выходному файлу
    :param dst_srs:
        Необходимая выходная проекция (SRS - Spatial reference system)
    :param format_file:
        Необходимый формат файла
    """
    if not dst_srs:
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(settings.DESTSRID)

    gv = gdal.VectorTranslateOptions(format=format_file, dstSRS=dst_srs)
    try:
        gdal.VectorTranslate(destNameOrDestDS=dst_path, srcDS=src_path, options=gv)
    except Exception as e:
        logger.critical('Невозможно перепроецировать вектор. Ошибка: {}'.format(e))
        sys.exit(-1)


def vector_rasterization(src_path: str, dst_path: str, bounds: list, x_res: int = 20, dst_srs: str = None,
                         y_res: int = 20, burn_values: int = 1, format_file: str = FORMAT_GEOTIFF) -> None:
    """ Растеризация входного вектора

        :param src_path:
            Путь к входному файлу
        :param dst_path:
            Путь к выходному файлу
        :param dst_srs:
            Необходимая выходная проекция (SRS - Spatial reference system)
        :param format_file:
            Необходимый формат файла
        :param x_res:
            Размеры изображения с единицами измерения при геопривязке. (По умолчанию - 20)
        :param y_res:
            Размеры изображения с единицами измерения при геопривязке. (По умолчанию - 20)
        :param bounds:
            Границы растеризации
        :param burn_values:
            Значение пикселей, на которое будут заменены полигоны. По умолчанию - 1
    """
    if not dst_srs:
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(settings.DESTSRID)

    gr = gdal.RasterizeOptions(format=format_file, outputSRS=dst_srs, xRes=x_res, yRes=y_res,
                               burnValues=burn_values, outputType=gdal.GDT_Int16, noData=0, outputBounds=bounds)
    try:
        gdal.Rasterize(destNameOrDestDS=dst_path, srcDS=src_path, options=gr)
    except RuntimeError as e:
        logger.critical(f'Невозможно выполнить растеризацию полигона. Ошибка: {e}')
        sys.exit(-1)


def create_file_from_array(src_ds: gdal.Dataset, dst_path: str,
                           np_array: np.array, format_file: str = FORMAT_GEOTIFF, band: int = 1,
                           data_type: any = None, nodata: int = None) -> None:
    """ Создание файла из массива пикселей

    :param src_ds:
        Входной gdal.Dataset
    :param dst_path:
        Путь к выходному файлу
    :param np_array:
        Numpy массив пикселей изображения
    :param format_file:
        Выходной формат файла. По умолчанию - GeoTIFF
    :param band:
        Группа пикселей по которым идёт обработка
    :param data_type:
        Формат данных
    :param nodata:
        Значение пикселей, которое будет записано в NODATA
    """
    try:
        if not data_type:
            data_type = src_ds.GetRasterBand(1).DataType

        driver = gdal.GetDriverByName(format_file)
        y_size, x_size = np_array.shape
        file = driver.Create(dst_path, x_size, y_size, 1, data_type)

        file.GetRasterBand(band).WriteArray(np_array)

        if nodata:
            file.GetRasterBand(band).SetNoDataValue(nodata)

        # Делаем привязку по координатам и проекциям
        proj = src_ds.GetProjection()
        georef = src_ds.GetGeoTransform()
        file.SetProjection(proj)
        file.SetGeoTransform(georef)
        file.FlushCache()
    except RuntimeError as e:
        logger.critical(f'Невозможно создать изображение из массива данных. Ошибка: {e}')
        sys.exit(-1)


def polygonize_raster(src_path: str, dst_path: str, format_file: str = FORMAT_SHP,
                      dst_srs: osr.SpatialReference = None) -> None:
    """ Векторизация входного растра в вектор
    
    :param src_path:
        Путь к входному файлу
    :param dst_path:
        Путь к выходному файлу
    :param format_file:
        Формат выходного файла. По умолчанию - SHP
    :param dst_srs:
        Необходимая выходная проекция (SRS - Spatial reference system)
    """

    with OpenImage(src_path) as src_ds:
        try:
            srcband = src_ds.GetRasterBand(1)

            if not dst_srs:
                dst_srs = osr.SpatialReference()
                dst_srs.ImportFromEPSG(settings.DESTSRID)

            dst_layername = utils.get_filename(dst_path)
            drv = ogr.GetDriverByName(format_file)
            dst_ds = drv.CreateDataSource(dst_layername + format_file)
            dst_layer = dst_ds.CreateLayer(dst_layername, srs=dst_srs)

            gdal.Polygonize(srcband, None, dst_layer, -1, [], callback=None)

        except RuntimeError as e:
            logger.critical(f'Невозможно преобразовать растр в вектор. Ошибка: {e}')
            sys.exit(-1)
