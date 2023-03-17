""" Взаимодействие с базой данных"""

import logging
import sys
from datetime import datetime, timedelta
from os.path import abspath, dirname, join

import geojson as gj
import psycopg2
import psycopg2.extras

import settings
from utils import DBConnector

logger = logging.getLogger('__name__')
INPUT_DIR = join(dirname(abspath(__file__)), 'intermediate')

# Настройки для подключения к базе данных
db_config = {'host': '192.168.0.9',
             'user': 'geoadmin',
             'password': 'canopus',
             'database': 'gpgeo'}


def check_layer_date(new_date: datetime.date) -> bool:
    """ Сравнение даты обрабатываемого снимка с наличием снимка в базе данных"""
    with DBConnector(db_config) as cur:
        sql = 'SELECT * FROM public."Layer" where date in (%s)'
        try:
            cur.execute(sql, (new_date,))
            date = cur.fetchone()
            if date:
                return False
            else:
                return True

        except psycopg2.Error as e:
            logger.critical(f'Невозможно проверить наличие снимков. Ошибка запроса БД: {e}')
            sys.exit(-1)


def get_bounds_lats_lons(dstype: int, field_group: int = None, field_id: int = None,
                         year: datetime.date = datetime.now().year) -> list[int]:
    """ Получает координаты прямоугольника, охватывающего все поля, поля в определенной группе или отдельное поле.
        Геометрия полей актуальна для указанного года.

    :param dstype:
        Идентификатор системы пространственной привязки (SRID) для перепроецирования изображений
    :param field_group:
        Номер группы полей
    :param field_id:
        id необходимого поля
    :param year:
        Необходимый год
    :return:
        Список координат охвата геометрии выбранных полей
    :example:
    >>> get_bounds_lats_lons()
    [42.8301839398382, 49.8701089785355, 43.1517742739582, 50.0494886747578]
    """
    with DBConnector(db_config) as cur:
        sql = 'SELECT * FROM __geostl_get_boundpoints(%s, %s, %s, %s)'
        try:
            cur.execute(sql, (year, dstype, field_id, field_group))
            lats_lons = cur.fetchall()
            return [lats_lons[0][0], lats_lons[0][1], lats_lons[2][0], lats_lons[2][1]]

        except psycopg2.Error as e:
            logger.critical('Невозможно получить координаты прямоугольника, охватывающего '
                            f'все поля или поля в пределах выбранной группы. Ошибка запроса БД: {e}')
            sys.exit(-1)


def get_last_date_from_layer(field_group: int = None) -> datetime.date:
    """ Получает крайнюю дату из таблицы "Layer" и добавляет 1 день

    :param field_group:
        Номер группы
    """
    with DBConnector(db_config) as cur:
        sql = 'SELECT * FROM __geostl_get_last_date(%s)'
        try:
            cur.execute(sql, (field_group,))
            date = cur.fetchone()[0]
            if date:
                date = date + timedelta(days=1)
                return date

        except psycopg2.Error as e:
            logger.critical(f'Невозможно получить крайнюю дату. Ошибка запроса БД: {e}')
            sys.exit(-1)


def get_agro_fields_geojson(field_group: int, year: int = datetime.now().year) -> None:
    """ Получение geojson файла по выбранной группе

    :param field_group:
        Номер группы полей
    :param year:
        Выбранный год
    """
    with DBConnector(db_config) as cur:
        sql = 'SELECT * FROM public.__geo_get_fieldshape_json(%s, %s)'
        try:
            cur.execute(sql, (field_group, year))
            geojson = cur.fetchall()
            with open(join(settings.INPUT_DIR, f'agro{field_group}.geojson'), 'w') as file:
                gj.dump(geojson[0][0], file)

        except psycopg2.Error as e:
            logger.critical(f'Невозможно получить geojson файл из БД. Ошибка: {e}')
            sys.exit(-1)


def get_field_geojson(field_id: int, field_name: int, field_group: int, dst_path: str,
                      year: int = datetime.now().year) -> None:
    """ Получает geojson файл выбранного поля

    :param field_id:
        ID выбранного поля
    :param field_name:
        Название поля
    :param field_group:
        Номер группы полей
    :param dst_path:
        Выходной файл
    :param year:
        Выбранный год
    """
    with DBConnector(db_config) as cur:
        sql = 'SELECT * FROM public.__geo_get_field_shape(%s, %s)'
        try:
            cur.execute(sql, (field_id, year))
            geojson = cur.fetchall()
            try:
                with open(join(dst_path, f'A{field_group}_FIELD{field_name}.geojson'), 'w') as file:
                    gj.dump(geojson[0][0], file)
            except OSError:
                pass

        except psycopg2.Error as e:
            logger.critical(f'Невозможно получить geojson файл из БД. Ошибка: {e}')
            sys.exit(-1)


def get_fields_id_from_group(field_group: int, year: int = datetime.now().year) -> list:
    """ Получение списка полей по выбранному хозяйству

    :param field_group:
        Номер группы полей
    :param year:
        Выбранный год
    """
    with DBConnector(db_config) as cur:
        sql = 'SELECT * FROM public.__geo_get_fieldnames_for_agro_year(%s, %s)'
        try:
            cur.execute(sql, (field_group, year))
            fields_id = cur.fetchall()
            return fields_id
        except psycopg2.Error as e:
            logger.critical(f'Невозможно получить списки полей из базы данных. Ошибка запроса БД: {e}')


def insert_ndvi_values(field_group: int, field: list, date: datetime.date,
                       mean_ndvi, max_ndvi: float, min_ndvi: float) -> None:
    """ Заносит данные о значении NDVI для выбранного поля

    :param field_group:
        Номер группы полей
    :param field:
        Список, содержащий в себе информацию по полю
    :param date:
        Дата значения индекса
    :param mean_ndvi:
        Среднее значение NDVI по полю
    :param max_ndvi:
        Максимальное значение NDVI по полю
    :param min_ndvi:
        Минимальное значение NDVI по полю
    """
    with DBConnector(db_config) as cur:
        sql = 'INSERT INTO public."NdviValues"(id, fieldid, fieldcode, date, ndvimean, ndvimax, ndvimin) ' \
              'VALUES (%s ,%s, %s, %s, %s, %s, %s);'

        sql_id = 'SELECT max(id) FROM public."NdviValues"'

        sql_check = 'SELECT * FROM public."NdviValues" WHERE (date in (%s) AND fieldid in (%s))'

        try:
            cur.execute(sql_check, (date, field[0]))
            check = cur.fetchall()
            if not check:
                cur.execute(sql_id)
                ndvi_id = cur.fetchall()[0][0] + 1

                cur.execute(sql, (ndvi_id, field[0], f'A{field_group}/F{field[1]}', date,
                                  mean_ndvi, max_ndvi, min_ndvi))
            else:
                print(f'Значение NDVI для поля A{field_group}/F{field[1]} с датой {date} уже существует. Пропуск')
        except psycopg2.Error as e:
            logger.critical(f'Невозможно внести значения NDVI по полю A{field_group}/F{field[1]}. '
                            f'Ошибка запроса БД: {e}')
            sys.exit(-1)


def get_field_groups() -> list:
    """ Получает список групп полей"""
    with DBConnector(db_config) as cur:
        sql = 'SELECT * FROM __geostl_get_field_group_ids()'
        try:
            cur.execute(sql)
            groups = cur.fetchall()
            if groups:
                groups = [i[0] for i in groups]
            else:
                groups = []
            return groups

        except psycopg2.Error as e:
            logger.critical(f'Невозможно получить список групп полей. Ошибка запроса БД: {e}')
            sys.exit(-1)


def insert_layer(date: datetime.date, set_: str, resolution: int, agroid: int, name: str, satellite: str,
                 field_id: int = None, isgrouplayer: bool = False) -> None:
    """ Вносит данные о снимке в таблицу Layer"""

    with DBConnector(db_config) as cur:
        sql = 'INSERT INTO public."Layer"(date, set, resolution, agroid, fieldid, name, satellite, isgrouplayer) ' \
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        try:
            cur.execute(sql, (date, set_, resolution, agroid, field_id, name, satellite, isgrouplayer))
        except psycopg2.Error as e:
            # Код ошибки PostgreSQL - 23505: Слой уже существует в таблице "Layer". Пропуск
            if e.pgcode == '23505':
                pass
            else:
                logger.critical(f'Невозможно вставить слой. Ошибка запроса БД: {e}')
                sys.exit(-1)
