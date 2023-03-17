""" Скрипт расчёта значений NDVI для каждого поля в хозяйстве"""

__author__ = 'Maxim Ilmenskiy'
__date__ = 'May 2021'

import logging
import shutil
from os.path import exists, join

import numpy as np
from glob2 import glob

import dboperator as db
import processing.baseimage as bi
import settings
from processing.baseimage import OpenImage
from utils import get_data_from_archive_sentinel

logger = logging.getLogger('__name__')


def get_ndvi_fields(src_path: str, field_group: int, fields: list) -> None:
    """ Нарезка изображений ndvi для каждого поля

    :param src_path:
        Исходный NDVI файл по хозяйству
    :param field_group:
        Номер хозяйства
    :param fields:
        Список полей по выбранному хозяйству
    """
    for field in fields:
        db.get_field_geojson(field_id=field[0], dst_path=settings.NDVI_DIR, field_name=field[1],
                             field_group=field_group)

        geojson_path = join(settings.NDVI_DIR, f'A{field_group}_FIELD{field[1]}.geojson')
        dst_path = join(settings.NDVI_DIR, f'A{field_group}_FIELD{field[1]}_ndvi.tif')

        if exists(geojson_path):
            bi.clip_by_shp(src_path=src_path, dst_path=dst_path, mask_file_path=geojson_path, flag_pass=True)


def get_scl_fields(field_group: int, fields: list) -> None:
    """ Получение битовой маски облачности по каждому полю

    :param fields:
        Список полей по выбранному хозяйству
    :param field_group:
        Номер хозяйства
    """
    for field in fields:
        geojson = join(settings.NDVI_DIR, f'A{field_group}_FIELD{field[1]}.geojson')
        dst_path = join(settings.NDVI_DIR, f'A{field_group}_FIELD{field[1]}_scl.tif')

        if exists(geojson):
            try:
                shutil.copyfile(join(settings.INPUT_DIR, f'clouds_agro{field_group}.tif'),
                                join(settings.NDVI_DIR, f'clouds_agro{field_group}.tif'))
            except OSError:
                pass

            bi.clip_by_shp(src_path=join(settings.NDVI_DIR, f'clouds_agro{field_group}.tif'),
                           mask_file_path=geojson, dst_path=dst_path, x_res=10, y_res=10, flag_pass=True)


def subtraction_fields(field_group: int, fields: list) -> None:
    """ Проверка каждого поля на облачность. Обнуление невалидных пикселей

    :param field_group:
        Выбранная группа полей
    :param fields:
        Список полей
    """
    for field in fields:
        ndvi_field = join(settings.NDVI_DIR, f'A{field_group}_FIELD{field[1]}_ndvi.tif')
        scl_field = join(settings.NDVI_DIR, f'A{field_group}_FIELD{field[1]}_scl.tif')

        if exists(ndvi_field) and exists(scl_field):
            with OpenImage(ndvi_field, scl_field) as (src_ds_ndvi, src_ds_scl):
                ndvi_array = src_ds_ndvi.ReadAsArray().astype(np.float64)
                scl_array = src_ds_scl.ReadAsArray()

                scl_array[np.where(scl_array == 0)] = 2
                scl_array[np.where(scl_array == 1)] = 0

                dst_ndvi_array = ndvi_array - scl_array

                dst_ndvi_array[np.where(dst_ndvi_array < 0)] = -1

                bi.create_file_from_array(src_ds=src_ds_ndvi,
                                          dst_path=join(settings.NDVI_DIR, f'A{field_group}_F{field[1]}_new_ndvi.tif'),
                                          np_array=dst_ndvi_array)


def get_ndvi_values(field_group: int, field: list) -> tuple:
    """ Высчитывание значение NDVI по всему полю

    :return max_ndvi:
        Максимальное значение NDVI по полю
    :return min_ndvi:
        Минимальное значение NDVI по полю
    :return mean_ndvi:
        Среднее значение NDVI по полю
    """
    if exists(join(settings.NDVI_DIR, f'A{field_group}_F{field[1]}_new_ndvi.tif')):
        with OpenImage(join(settings.NDVI_DIR, f'A{field_group}_F{field[1]}_new_ndvi.tif')) as src_ds:

            ndvi_array = src_ds.ReadAsArray().astype(np.float64)

            max_ndvi = 0.0
            min_ndvi = 1.1
            count_ndvi = 0
            sum_ndvi = 0

            for band in ndvi_array:
                for value in band:
                    if 0 < value < 1:
                        count_ndvi += 1
                        sum_ndvi += value
                        if max_ndvi < value:
                            max_ndvi = value
                        if value < min_ndvi:
                            min_ndvi = value
            try:
                mean_ndvi = sum_ndvi / count_ndvi
            except ZeroDivisionError:
                max_ndvi, min_ndvi, mean_ndvi = None, None, None

        return max_ndvi, min_ndvi, mean_ndvi


def ndvivalue() -> None:
    """ Основная функция скрипта формирования значений NDVI по полям"""
    for field_group in range(1, 7):
        ndvi_file = glob(join(settings.INPUT_DIR, f's2[a-b]_??_??_????_a{field_group}_ndvi_10m_3857.tif'))
        if ndvi_file:
            print(f'Получение значений NDVI в Агро {field_group} каждому полю...')
            # Получаем tiff ndvi файлы по каждому полю (идёт обрезка по geojson файлу из базы данных)
            fields = db.get_fields_id_from_group(field_group=field_group)
            get_ndvi_fields(src_path=ndvi_file[0], field_group=field_group, fields=fields)

            # Получаем tiff маску облачности по каждому полю
            get_scl_fields(field_group=field_group, fields=fields)

            # Делаем вычитание маски облачности и приводим tiff ndvi в нужный вид
            subtraction_fields(field_group=field_group, fields=fields)
            for field in fields:
                if exists(join(settings.NDVI_DIR, f'A{field_group}_F{field[1]}_new_ndvi.tif')):
                    # Высчитываем значения ndvi по каждому полю
                    max_ndvi, min_ndvi, mean_ndvi = get_ndvi_values(field_group=field_group, field=field)

                    # Заносим значения ndvi по каждому полю в базу данных
                    if max_ndvi and min_ndvi and mean_ndvi:
                        # Получаем дату снимка
                        files = glob(join(settings.DOWNLOADS_DIR, '*.zip'))
                        date = get_data_from_archive_sentinel(file_path=files[0])[1]

                        db.insert_ndvi_values(field_group=field_group, date=date, field=field,
                                              mean_ndvi=mean_ndvi, min_ndvi=min_ndvi, max_ndvi=max_ndvi)
    print('Процесс по внесению данных NDVI окончен')


if __name__ == '__main__':
    ndvivalue()
