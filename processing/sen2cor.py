#! /usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Maxim Ilmenskiy'
__date__ = 'May 2021'

import logging
from datetime import datetime
from os import system
from os.path import basename, join

import numpy as np
from glob2 import glob
from osgeo import gdal

import processing.baseimage as bi
import settings
from processing.baseimage import OpenImage
from utils import get_data_from_archive_sentinel, unzip

logger = logging.getLogger('__name__')
# Делаем возможность игнорировать деление на ноль
np.seterr(divide='ignore', invalid='ignore')


def get_ndvi_image(red_file: str, nir_file: str, dst_path: str) -> None:
    """ Формирование NDVI изображения из уровня L1C обработки снимка

    :param red_file:
        Входной B04 спектр
    :param nir_file:
        Входной B08 спектр
    :param dst_path:
        Путь к выходному файлу
    """

    with OpenImage(red_file, nir_file) as (red_link, nir_link):
        red = red_link.ReadAsArray().astype(np.float64)
        nir = nir_link.ReadAsArray().astype(np.float64)

        ndvi = (nir - red) / (nir + red)

    bi.create_file_from_array(src_ds=red_link, np_array=ndvi, dst_path=dst_path, band=1,
                              data_type=gdal.GDT_Float64)


def get_ndre_image(nir_file: str, red_edge_file: str, dst_path: str) -> None:
    """ Формирование NDRE изображения из уровня L1C обработки снимка

    :param red_edge_file:
        Входной B07 спектр
    :param nir_file:
        Входной B08 спектр
    :param dst_path:
        Путь к выходному файлу
    """

    with OpenImage(nir_file, red_edge_file) as (nir_link, red_edge_link):
        nir = nir_link.ReadAsArray().astype(np.float64)
        red_edge = red_edge_link.ReadAsArray().astype(np.float64)

        ndre = (nir - red_edge) / (nir + red_edge)

    bi.create_file_from_array(src_ds=nir_link, np_array=ndre, dst_path=dst_path, band=1, nodata=-1,
                              data_type=gdal.GDT_Float64)


def sen2cor() -> None:
    """ Функция обработки архива со снимком"""
    # Архив со снимком
    archives = glob(join(settings.DOWNLOADS_DIR, '*.zip'))
    if archives:
        # Распаковываем архив со снимком
        print(f'Разархивирование снимка {basename(archives[0])}')
        unzip(src_path=archives[0], dst_path=settings.TEMP_PROCESSING_DIR)

        # Папка со снимком
        safe_files = glob(join(settings.TEMP_PROCESSING_DIR, '*.SAFE'))
        if safe_files:
            # Запускаем обработчик sen2cor
            system(command=settings.SEN2COR_PATH + f' --resolution 10 --GIP_L2A L2A_GIPP.xml {safe_files[0]}')
            # /home/sysop/sentinel/temp_for_processing/S2A_MSIL1C_20230206T081121_N0509_R078_T38ULA_20230206T090535.SAFE/GRANULE/L1C_T38ULA_A039829_20230206T081235/IMG_DATA
            # Дата снимка
            image_date = datetime.strftime(get_data_from_archive_sentinel(file_path=archives[0])[1], '%d_%m_%Y')

            # Получаем RGB изображение в проекции EPSG:32638:
            print('Получение RGB изображения из уровня L1C снимка...')
            rgb_file = glob(join(settings.TEMP_PROCESSING_DIR,
                                 'S2?_MSIL2A*', 'GRANULE', 'L2*',
                                 'IMG_DATA', 'R10m', '*TCI_10m.jp2'))

            rgb_name = join(settings.INPUT_DIR, f'rgb_{image_date}.tif')
            bi.projection_raster(src_path=rgb_file[0], dst_path=rgb_name, dst_srs='EPSG:32638')
            print('RGB успешно получено')

            # Получаем SCL изображение в проекции EPSG:32638:
            print('Получение SCL изображения из уровня L2 снимка...')
            scl_file = glob(join(settings.TEMP_PROCESSING_DIR,
                                 'S2?_MSIL2A*', 'GRANULE', 'L2*',
                                 'IMG_DATA', 'R20m', '*SCL_20m.jp2'))

            scl_name = join(settings.INPUT_DIR, f'scl_{image_date}.tif')
            bi.projection_raster(src_path=scl_file[0], dst_path=scl_name, dst_srs='EPSG:32638')
            print('SCL успешно получено')

            # Получаем NDVI изображение
            print('Получение NDVI изображения из уровня L1C...')
            red_file = glob(join(settings.TEMP_PROCESSING_DIR,
                                 'S2?_MSIL1C*', 'GRANULE',
                                 'L1*', 'IMG_DATA', '*B04.jp2'))

            nir_file = glob(join(settings.TEMP_PROCESSING_DIR,
                                 'S2?_MSIL1C*', 'GRANULE',
                                 'L1*', 'IMG_DATA', '*B08.jp2'))

            ndvi_name = join(settings.INPUT_DIR, f'ndvi_{image_date}.tif')
            get_ndvi_image(red_file=red_file[0], nir_file=nir_file[0], dst_path=ndvi_name)
            print('NDVI успешно получено')

            # @TODO - необходимо добавить эту функцию в скрипт и добавить снимок NDRE на Geo
            # @TODO - пока что, эта функция не реализована из-за невозможности добавить на старый сайт новый тип снимков
            """ # Получаем NDRE изображение
            print('Получение NDRE...')
            red_edge_file = glob(join(settings.TEMP_PROCESSING_DIR,
                                      'S2*_MSIL1C*', 'GRANULE',
                                      'L1*', 'IMG_DATA', '*B05.jp2'))
            nir_nirrow_file = glob(join(settings.TEMP_PROCESSING_DIR,
                                        'S2*_MSIL1C*', 'GRANULE',
                                        'L1*', 'IMG_DATA', '*B8A.jp2'))
            ndre_name = join(settings.INPUT_DIR, f'ndre_{image_date}.tif')
            get_ndre_image(nir_file=nir_nirrow_file[0], red_edge_file=red_edge_file[0], dst_path=ndre_name)"""

        else:
            raise SystemExit('Директорий со снимками для обработки необнаружено. Скрипт отключён')
    else:
        raise SystemExit('Архивов со снимками необнаружено. Скрипт отключён')


if __name__ == '__main__':
    sen2cor()
