""" Функция нарезки снимков по Агро"""
# -*- coding: utf-8 -*-

__author__ = 'Vladimir Salnikov, Maxim Ilmenskiy'
__date__ = 'March 2021'

import argparse
import sys
from datetime import datetime
from os import walk
from os.path import exists, join

from glob2 import glob
from osgeo import gdal, osr

import dboperator as db
import processing.baseimage as bi
import settings
import utils as ut
from processing.baseimage import OpenImage
from processing.baseimage.colormap import ColorMap


def process(group: int, file: str, set_: str, date: str, dst_srs: osr.SpatialReference, satellite: str) -> None:
    """ Обработка входных изображений по группам

    :param group:
        Номер группы полей
    :param file:
        Путь к файлу
    :param set_:
        Тип изображения
    :param date:
        Дата снимка
    :param dst_srs:
        Выходная проекция файла
    :param satellite:
        Номер спутника (s2a или s2b)
    """
    bounds = db.get_bounds_lats_lons(year=settings.YEAR, field_group=group, dstype=settings.DESTSRID)

    with OpenImage(file) as src_ds:
        src_srs = osr.SpatialReference(wkt=src_ds.GetProjection()) if dst_srs else None
        gt = src_ds.GetGeoTransform()
        res = gt[1]
        bounds = bi.find_band_bounds(bounds, gt, src_ds.RasterXSize, src_ds.RasterYSize, src_srs, dst_srs)
        print('Новые границы: {}.'.format(bounds))
        if bounds[0] > bounds[2] or bounds[1] > bounds[3]:
            print('Зона пустая. Пропуск файла')
            return

    # Поскольку поля в Агро 1 попадают сразу на два снимка, то необходимо сначала их объединить
    # Для этого, создаются отдельные файлы
    if group == 1:
        dst_path = join(settings.COMBINE_DIR, f'combine1_{satellite}_{date}_a{group}_{set_}_10m_3857.tif')
        if exists(dst_path):
            dst_path = join(settings.COMBINE_DIR, f'combine2_{satellite}_{date}_a{group}_{set_}_10m_3857.tif')
    else:
        dst_path = join(settings.INPUT_DIR, f'{satellite}_{date}_a{group}_{set_}_10m_3857.tif')

    print('Попытка нарезки участка...')
    gdal.Warp(dst_path, src_ds, format=bi.FORMAT_GEOTIFF, outputBounds=bounds, outputBoundsSRS=dst_srs, xRes=res,
              yRes=res, srcSRS=src_srs, dstSRS=dst_srs, resampleAlg=gdal.GRIORA_Lanczos, srcNodata=settings.NODATA,
              dstNodata=settings.NODATA)
    print('Участок готов')


def set_args() -> argparse.ArgumentParser:
    """Установка аргументов запуска скрипта"""

    def valid_groups(s) -> list:
        """ Проверка значения на валидность группы

        :param s:
            Входное значение групп в виде строки, через запятую
        :return:
            Список групп, по которым будет проводиться обработка снимка
        """
        try:
            return [ut.check_positive_int(x) for x in s.split(',')]
        except ValueError as er:
            raise argparse.ArgumentTypeError(er)

    parser = argparse.ArgumentParser(prog='sentinel')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-g', '--groups', type=valid_groups,
                       help='Номер группы полей для обработки снимка. '
                            'По умолчанию используются все доступные в БД группы. '
                            'Пример: --groups 1,2,3,7')
    group.add_argument('-ng', '--no_groups', action='store_true',
                       help='Обработка производится без использования групп полей. По умолчанию - False.')
    parser.add_argument('-wm', '--with_merge', action='store_true',
                        help='Обработка изображений по Агро 1 будет производиться с объединением снимков')
    return parser


def sentinel(field_groups: list = None) -> None:
    """Основная функция управления скриптом"""
    if field_groups is None:
        field_groups = db.get_field_groups()

    archives = glob(join(settings.DOWNLOADS_DIR, '*.zip'))
    image_date = datetime.strftime(ut.get_data_from_archive_sentinel(file_path=archives[0])[1], '%d_%m_%Y')
    satellite = ut.get_data_from_archive_sentinel(file_path=archives[0])[0].lower()

    files = {'ndvi': [], 'rgb': [], 'scl': []}
    for (dirpath, dirnames, filenames) in walk(settings.INPUT_DIR):
        files['ndvi'] = [join(settings.INPUT_DIR, file) for file in filenames if 'ndvi' in str.lower(file)]
        files['rgb'] = [join(settings.INPUT_DIR, file) for file in filenames if 'rgb' in str.lower(file)]
        files['scl'] = [join(settings.INPUT_DIR, file) for file in filenames if 'scl' in str.lower(file)]
        break

    print(files)

    try:
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(settings.DESTSRID)
    except RuntimeError as e:
        print(f'Неизвестный SRID. Ошибка GDAL: {e}.')
        sys.exit(1)

    cm = ColorMap(settings.COLORMAP, settings.NODATA)
    cm.parse_color_map()

    for group in field_groups:
        for set_, files_in_set in files.items():
            for file in files_in_set:
                print(f'[Нарезка снимков]: Группа: {group}, Тип: {set_}, Файл: {file}.')
                process(group, file, set_, image_date, dst_srs, satellite)


if __name__ == '__main__':
    sentinel()
