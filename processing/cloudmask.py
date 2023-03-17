#! /usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Maxim Ilmenskiy'
__date__ = 'April 2021'

import logging
import shutil
import sys
from datetime import datetime
from os import remove
from os.path import join

import numpy as np
from glob2 import glob
from osgeo import osr

import dboperator as db
import processing.baseimage as bi
import settings
from processing.baseimage import OpenImage
from processing.baseimage.colormap import ColorMap
from utils import get_data_from_archive_sentinel

logger = logging.getLogger('__name__')


def resize_shape(array_a: np.array, array_b: np.array) -> tuple:
    """ Выравнивает размеры массивов, если они расходятся

    :param array_a:
        Входной массив данных для выравнивания
    :param array_b:
        Входной массив
    :returns:
        Кортеж исправленных массивов
    """
    shape_a = array_a.shape
    shape_b = array_b.shape
    print(f'Изначальные размеры:\n Массив A: {shape_a}\n Массив B: {shape_b}')
    if shape_a[0] != shape_b[0] and shape_a[1] == shape_b[1]:
        if shape_a[0] > shape_b[0]:
            diff = shape_a[0] - shape_b[0]
            new_array_b = np.r_[array_b, np.zeros((diff, shape_a[1]))]
            print(f'Полученные размеры:\n Массив A: {shape_a}\n Массив B: {new_array_b.shape}')
            return array_a, new_array_b
        else:
            diff = shape_b[0] - shape_a[0]
            new_array_a = np.r_[array_a, np.zeros((diff, shape_b[1]))]
            print(f'Полученные размеры:\n Массив A: {new_array_a.shape}\n Массив B: {shape_b}')
            return new_array_a, array_b

    elif shape_a[0] == shape_b[0] and shape_a[1] != shape_b[1]:
        if shape_a[1] > shape_b[1]:
            diff = shape_a[1] - shape_b[1]
            new_array_b = np.r_[array_b, np.zeros((shape_a[0], diff))]
            print(f'Полученные размеры:\n Массив A: {shape_a}\n Массив B: {new_array_b.shape}')
            return array_a, new_array_b
        else:
            diff = shape_b[1] - shape_a[1]
            new_array_a = np.r_[array_a, np.zeros((shape_b[0], diff))]
            print(f'Полученные размеры:\n Массив A: {new_array_a.shape}\n Массив B: {shape_b}')
            return new_array_a, array_b
    else:
        return array_a, array_b


def check_clouds_on_tiff(src_path_a: str, src_path_b: str, field_group: int) -> float:
    """ Проверка видимости пикселей. Считаются валидные пиксели во всех масках.
        Берётся процент от общего количества валидных пикселей на маске полей.

    :param src_path_a:
        Входной файл маски полей
    :param src_path_b:
        Входной файл маски после вычета
    :param field_group:
        Номер хозяйства
    :return:
        Процент полей, который видно на снимке
    """
    with OpenImage(src_path_a, src_path_b) as (src_ds_a, src_ds_b):
        try:
            # Переменные под количество валидных пикселей
            pixels_mask = 0
            pixels_subtraction = 0

            # Считаем пиксели в маске полей
            array_mask = src_ds_a.ReadAsArray().astype(np.int16)
            for line in array_mask:
                for pixel in line:
                    if pixel == 2:
                        pixels_mask += 1

            # Считаем количество валидных пикселей после вычитания
            array_subtraction = src_ds_b.ReadAsArray().astype(np.int16)
            for line in array_subtraction:
                for pixel in line:
                    if pixel == 1:
                        pixels_subtraction += 1

            percent = round((pixels_subtraction / (pixels_mask / 100)), 1)

            print(f'\nКоличество пикселей в маске полей по хозяйству Агро{field_group}: {pixels_mask}\n'
                  f'Количество пикселей в маске облачности (которые видно) '
                  f'по хозяйству Агро{field_group}: {pixels_subtraction}\n'
                  f'Процент видимых пикселей: {percent}%')
            return percent

        except Exception as e:
            logger.critical(f'Невозможно подсчитать процент видимости полей. Ошибка: {e}')
            sys.exit(-1)


def get_bit_mask_from_scl(src_path: str, dst_path: str):
    """ Реклассификация scl изображения в битовую маску, где:
        1 - валидные пиксели
        0 - невалидные пиксели.

        У полученной, через sen2cor, маски облачности имеется 12 классификаций пикселей.
        Пиксели, классифицированные как 4, 5, 6, 7, 11, 12 являются валидными пикселями, всё остальное - обнуляем.

    :param src_path:
        Входной файл
    :param dst_path:
        Выходной файл
    """
    with OpenImage(src_path) as src_ds:
        scl_array = src_ds.ReadAsArray().astype(np.int16)

        # Реклассифицируем пиксели
        scl_array[np.where(scl_array < 4)] = 0
        scl_array[np.where(scl_array == 4)] = 1
        scl_array[np.where(scl_array == 5)] = 1
        scl_array[np.where(scl_array == 6)] = 1
        scl_array[np.where(scl_array == 7)] = 1
        scl_array[np.where(scl_array == 8)] = 0
        scl_array[np.where(scl_array == 9)] = 0
        scl_array[np.where(scl_array == 10)] = 0
        scl_array[np.where(scl_array == 11)] = 1
        scl_array[np.where(scl_array == 12)] = 1
        scl_array[np.where(scl_array > 12)] = 0

    bi.create_file_from_array(src_ds=src_ds, dst_path=dst_path, np_array=scl_array, nodata=0)


def subtraction_for_fields(src_path_a: str, src_path_b: str, dst_path: str) -> None:
    """ Калькулятор растров для битовых масок. Происходит вычитание маски облачности из маски полей

        :param src_path_a:
            Входной файл маски полей (битовая)
        :param src_path_b:
            Входной файл маски облачности (битовая)
        :param dst_path:
            Путь к выходному файлу
    """
    with OpenImage(src_path_a, src_path_b) as (src_ds_a, src_ds_b):
        data_a = src_ds_a.ReadAsArray().astype(np.int16)
        data_b = src_ds_b.ReadAsArray().astype(np.int16)

        # Делаем вычитание
        try:
            scl_array = data_a - data_b
        except ValueError:
            print('Размеры изображений не совпадают. Для устранения ошибки, '
                  'к краю меньшего изображения будут добавлены пиксели, со значениями 0')
            new_arrays = resize_shape(array_a=data_a, array_b=data_b)
            try:
                scl_array = new_arrays[0] - new_arrays[1]
            except ValueError:
                raise SystemExit('Размеры изображений всё так же не совпадают. Скрипт отключён')

        # Делаем реклассификацию для избежания проблем с отображением невалидных пикселей
        scl_array[np.where(scl_array < 0)] = 0
        scl_array[np.where(scl_array == 1)] = 1
        scl_array[np.where(scl_array > 1)] = 0

        bi.create_file_from_array(src_ds=src_ds_a, dst_path=dst_path, np_array=scl_array, nodata=0)


def get_ndvi_with_cloud_mask(cm: any, field_group: int, src_path_ndvi: str, src_path_scl: str) -> None:
    """ Переклассификация пикселей на снимке NDVI, для адекватного отображения облачности.
        Так же, здесь идёт разукрашивание NDVI в цветовую схему
    """
    bounds = db.get_bounds_lats_lons(year=datetime.now().year,
                                     field_group=field_group,
                                     dstype=settings.DESTSRID)

    with OpenImage(src_path_ndvi) as src_ds:
        src_srs = osr.SpatialReference(wkt=src_ds.GetProjection())
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(settings.DESTSRID)
        gt = src_ds.GetGeoTransform()
        lats_lons = bi.find_band_bounds(bounds, gt, src_ds.RasterXSize, src_ds.RasterYSize, src_srs, dst_srs)

    ll_lon = lats_lons[0]
    ll_lat = lats_lons[1]
    ur_lon = lats_lons[2]
    ur_lat = lats_lons[3]

    if ll_lat and ll_lon and ur_lat and ur_lon:
        geometry = f'POLYGON (({ur_lon} {ll_lat}, {ur_lon} {ur_lat}, ' \
                   f'{ll_lon} {ur_lat}, {ll_lon} {ll_lat}, {ur_lon} {ll_lat}))'

        dst_path_footprint = join(settings.INPUT_DIR, f'footprint_clouds_a{field_group}.shp')
        dst_path_clouds = join(settings.INPUT_DIR, f'clouds_10m_a{field_group}.tif')

        bi.create_shape_file_from_wkt(wkt_geometry=geometry,
                                      dst_path=dst_path_footprint)

        bi.clip_by_shp(src_path=src_path_scl, mask_file_path=dst_path_footprint, x_res=10, y_res=10,
                       dst_path=dst_path_clouds)

        with OpenImage(dst_path_clouds, src_path_ndvi) as (src_clouds, src_ndvi):
            clouds_array = src_clouds.ReadAsArray().astype(np.int16)

            clouds_array[np.where(clouds_array == 0)] = 2
            clouds_array[np.where(clouds_array == 1)] = 0

            ndvi_array = src_ndvi.ReadAsArray().astype(np.float64)

            dst_ndvi = ndvi_array - clouds_array

            dst_path = join(settings.INPUT_DIR, f'ndvi_clouds_a{field_group}_10m.tif')

            bi.create_file_from_array(src_ds=src_ndvi,
                                      dst_path=dst_path,
                                      np_array=dst_ndvi)

        cm.create_rgba(src_path=dst_path, dst_path=src_path_ndvi.split('.')[0] + '_img.tif')
    else:
        print('Невозможно получить границы охвата полей. Остановка...')
        sys.exit(-1)


def cloudmask() -> None:
    """Основная функция скрипта - это получение битовой маски облачности и процент видимых полей"""
    cm = ColorMap(settings.COLORMAP, settings.NODATA)
    cm.parse_color_map()

    print('Идёт процесс проверки на облачность...')
    for field_group in range(1, 7):
        scl_files = glob(join(settings.INPUT_DIR, f's2*a{field_group}_scl*.tif'))
        if scl_files:
            print(f'\nПопытка получения битовой маски облачности и маски полей по Агро {field_group}...')
            archives = glob(join(settings.DOWNLOADS_DIR, '*.zip'))
            image_date = datetime.strftime(get_data_from_archive_sentinel(file_path=archives[0])[1], '%d_%m_%Y')
            satellite = get_data_from_archive_sentinel(file_path=archives[0])[0].lower()

            db.get_agro_fields_geojson(field_group=field_group, year=datetime.now().year)

            # Перепроецируем полигон
            src_path = join(settings.INPUT_DIR, f'agro{field_group}.geojson')
            dst_path = join(settings.INPUT_DIR, f'fixed_agro{field_group}.geojson')

            bi.projection_vector(src_path=src_path, dst_path=dst_path, format_file=bi.FORMAT_GEOJSON)
            remove(src_path)

            # Растерезируем полигон (расширением 20м на пиксель)
            bounds = db.get_bounds_lats_lons(dstype=settings.DESTSRID, field_group=field_group)

            src_path = join(settings.INPUT_DIR, f'fixed_agro{field_group}.geojson')
            dst_path = join(settings.INPUT_DIR, f'fields_agro{field_group}.tif')

            bi.vector_rasterization(src_path=src_path, dst_path=dst_path, burn_values=2,
                                    x_res=20, y_res=20, format_file=bi.FORMAT_GEOTIFF, bounds=bounds)
            remove(src_path)

            # Получаем битовую маску из scl изображения
            src_path = join(settings.INPUT_DIR, f'{satellite}_{image_date}_a{field_group}_scl_10m_3857.tif')
            dst_path = join(settings.INPUT_DIR, f'clouds_agro{field_group}.tif')

            get_bit_mask_from_scl(src_path=src_path, dst_path=dst_path)

            # Делаем вычитание масок
            src_path_a = join(settings.INPUT_DIR, f'fields_agro{field_group}.tif')
            src_path_b = join(settings.INPUT_DIR, f'clouds_agro{field_group}.tif')
            dst_path = join(settings.INPUT_DIR, f'subtraction_agro{field_group}.tif')
            subtraction_for_fields(src_path_a=src_path_a, src_path_b=src_path_b, dst_path=dst_path)

            # Высчитываем процент видимых пикселей (сколько процентов площади не закрыто облаками и прочим)
            src_path_a = join(settings.INPUT_DIR, f'fields_agro{field_group}.tif')
            src_path_b = join(settings.INPUT_DIR, f'subtraction_agro{field_group}.tif')
            percent = check_clouds_on_tiff(src_path_a=src_path_a, src_path_b=src_path_b, field_group=field_group)

            if percent >= 75:
                print(f'Снимок по Агро {field_group} пригоден для просмотра.')

                src_path_ndvi = join(settings.INPUT_DIR, f'{satellite}_{image_date}_a{field_group}_ndvi_10m_3857.tif')
                src_path_scl = join(settings.INPUT_DIR, f'clouds_agro{field_group}.tif')

                # Проверка значений NDVI на облачность
                print(f'Проверка значений NDVI на облачность в Агро {field_group}')
                get_ndvi_with_cloud_mask(field_group=field_group, src_path_ndvi=src_path_ndvi,
                                         src_path_scl=src_path_scl, cm=cm)
                print(f'Снимок NDVI по Агро {field_group} прошёл проверку пикселей... Файлы готовится к публикации')
                ndvi_file = f'{satellite}_{image_date}_a{field_group}_ndvi_10m_3857_img.tif'
                rgb_file = f'{satellite}_{image_date}_a{field_group}_rgb_10m_3857.tif'

                shutil.copy(join(settings.INPUT_DIR, ndvi_file), join(settings.PROCESSED_DIR, ndvi_file))
                shutil.copy(join(settings.INPUT_DIR, rgb_file), join(settings.PROCESSED_DIR, rgb_file))
                print(f'Снимки по Агро {field_group} готовы к публикации в {settings.PROCESSED_DIR}')
            else:
                print(f'Снимки по Агро {field_group} непригоден для просмотра. Пропуск...')


if __name__ == '__main__':
    cloudmask()
