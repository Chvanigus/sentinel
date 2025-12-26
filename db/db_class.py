"""Диспетчер контекста для подключения и работы с базой данных."""
import dataclasses
import os
import sys
from datetime import datetime
from typing import List, Optional, Type, Tuple

import geojson as gj
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, DictRow, execute_batch

from core.logging import get_logger
from db.data_class import NdviValues, Field, Layer
from db.utils import get_count_s


class PostgisConnector:
    """
    Класс для получения и сохранения данных из БД.
    """

    def __init__(self, pg_conn: _connection) -> None:
        self.con = pg_conn
        self.cur = self.con.cursor(cursor_factory=DictCursor)
        self.logger = get_logger()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur:
            self.cur.close()
        if self.con:
            self.con.close()

    @staticmethod
    def _get_count_s(table: str, id_flag: bool = False) -> str:
        """
        Возвращает количество %s в виде строки формата: '%s,%s...'
        :param table: Таблица описанная как dataclasses.dataclass
        :param id_flag: Учитывать поле ID или нет
        :return: Строка вида '%s,%s...'
        """
        return get_count_s(table=table, id_flag=id_flag)

    @staticmethod
    def _get_field_name(table: dataclasses.dataclass) -> list:
        """
        Возвращает поля таблицы.
        :param table: Таблица описанная как dataclasses.dataclass
        :return: Список полей таблицы
        """
        return table.__dataclass_fields__.keys()

    def _get_query_to_save(
            self, table, id_flag: bool = False,
            on_conflict_fields: str = "id"
    ) -> str:
        """
        Возвращает SQL запрос для сохранения данных в БД.
        :param table: Таблица описанная через dataclasses.dataclass.
        :param id_flag: Учитывать флаг ID или нет.
        :param on_conflict_fields: Строка в виде набора полей,
                                   которые учитываются при конфликте данных.
        :return: Строка запроса для сохранения данных в БД.
        """
        count_s = self._get_count_s(table=table, id_flag=id_flag)
        field_names = self._get_field_name(table)

        if id_flag:
            query = f"""
                INSERT INTO "gpgeo"."{table.TableName()}" 
                VALUES ({count_s}) 
                ON CONFLICT ({on_conflict_fields}) DO NOTHING;
                """
        else:
            field_names = filter(lambda x: x != 'id', field_names)
            query = f"""
                    INSERT INTO "gpgeo"."{table.TableName()}" 
                    ({", ".join(field_names)}) 
                    VALUES ({count_s})
                    ON CONFLICT ({on_conflict_fields}) DO NOTHING;
                    """

        return query

    def get_tuples_data_for_save(
            self, table: dataclasses.dataclass, data: list
    ) -> List[tuple] or None:
        """
        Возвращает список данных преобразованных в кортеж для сохранения в БД.
        :param table: Таблица описанная через dataclasses.dataclass.
        :param data: Список данных для сохранения в БД.
        :return: Список кортежей для сохранения в БД.
        """
        tuple_data = []
        try:
            for it in data:
                if isinstance(it, tuple):
                    tuple_data.append(it)
                else:
                    tuple_data.append(it.to_tuple())
            return tuple_data
        except AttributeError:
            self.logger.error(
                f"Не удалось преобразовать данные в кортеж. "
                f"Проверьте, что данные переданы в виде списка "
                f"объектов класса {table.__name__} или в виде списка кортежей."
            )
            return None

    def extract_all(
            self, query: str, _vars: tuple = None
    ) -> Optional[List[DictRow]]:
        """
        Экстрактор для получения списка данных.
        :param query: SQL запрос
        :param _vars: Кортеж переменных для запроса.
                      Порядок переменных должен соответствовать порядку полей
                      в таблице.
        """
        try:
            self.cur.execute(query, _vars)
            return self.cur.fetchall()
        except psycopg2.Error as e:
            self.logger.critical(f"Ошибка при попытке выполнения запроса: {e}")
            sys.exit(-1)

    def extract_one(
            self, query: str, _vars: tuple = None
    ) -> Optional[DictRow]:
        """
        Экстрактор для получения единичной записи.
        :param query: SQL запрос
        :param _vars: Кортеж переменных запроса.
                      Порядок переменных должен соответствовать порядку полей
                      в таблице.
        """
        try:
            self.cur.execute(query, _vars)
            return self.cur.fetchone()
        except psycopg2.Error as e:
            self.logger.critical(f"Ошибка при попытке выполнения запроса: {e}")
            sys.exit(-1)

    def save_one(
            self, table: dataclasses.dataclass, _vars: tuple,
            id_flag: bool = False,
            on_conflict_fields: str = "id"
    ) -> bool:
        """
        Сохранение одной записи для переданного запроса.
        :param table: Таблица описанная через Dataclass, в которую
                      требуется сохранить данные.
        :param _vars: Кортеж переменных для запроса.
                      Порядок переменных должен соответствовать порядку полей
                      в таблице.
        :param id_flag: True если нужно ID поле. False - если не надо.
        :param on_conflict_fields: Строка в виде набора полей,
                                   которые учитываются при конфликте данных.
        """
        query = self._get_query_to_save(
            table=table, id_flag=id_flag,
            on_conflict_fields=on_conflict_fields
        )

        try:
            self.cur.execute(query, _vars)
            self.con.commit()
            return True
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка при попытке выполнения запроса: {e}")
            self.con.commit()
            sys.exit(-1)

    def save_all(
            self, table: dataclasses.dataclass,
            data: List[Type[dataclasses.dataclass]] or List[Tuple],
            id_flag: bool = False,
            on_conflict_fields: str = "id"
    ) -> bool:
        """
        Сохранение списка записей для переданного запроса.
        :param table: Таблица описанная через Dataclass, в которую
                      требуется сохранить данные.
        :param data:  Список записей для сохранения.
        :param id_flag: True если нужно ID поле. False - если не надо.
        :param on_conflict_fields: Список полей в виде строки, которые
                                   учитываются при внесении данных.
                                   Если будет конфликт данных -
                                   запись будет пропущена.
        """
        tuple_data = self.get_tuples_data_for_save(table, data)
        if tuple_data is None:
            return False

        query = self._get_query_to_save(
            table=table, id_flag=id_flag,
            on_conflict_fields=on_conflict_fields
        )

        try:
            execute_batch(self.cur, query, tuple_data, page_size=100)
            self.con.commit()
            return True
        except psycopg2.Error as err:
            self.logger.error(f"{err}")
            self.con.commit()
            return False


class PostgisWorker:
    """Класс для работы с базой данных в проекте SENTINEL."""

    def __init__(self, connector: PostgisConnector) -> None:
        self.conn = connector

    def check_layer(self, layer: Layer) -> bool:
        """
        Проверяет наличие спутникового снимка.
        :param layer: Объект дата-класса Layer для проверки.
        :return: Булевое значение, что снимок существует или нет
        """
        query = """
        SELECT * FROM gpgeo."maps_layer" WHERE 
        date IN (%s) AND agroid IN (%s) AND set IN (%s)
        """

        if self.conn.extract_one(
                query=query,
                _vars=(layer.date, layer.agroid, layer.set)
        ):
            return True
        else:
            return False

    def check_layer_date(self, date: datetime) -> bool:
        """
        Проверяет наличие спутникового снимка с переданной датой.
        :param date: Дата для проверки.
        :return: Булевое значение, что снимок существует или нет
        """
        query = """SELECT * FROM gpgeo."maps_layer" WHERE date IN (%s)"""

        if self.conn.extract_one(query, (date,)):
            return True
        else:
            return False

    def get_fieldids_from_agro(
            self, agroid: int, year: int = datetime.now().year
    ) -> List[Field]:
        """
        Возвращает список ID полей в выбранном агропредприятии.
        :param agroid: ID агропредприятия.
        :param year: Год, по которому идёт выборка.
        :return: Список ID полей, которые входят в выбранное агропредприятие
        """
        query = """
        SELECT * FROM gpgeo.__geo_get_fieldnames_for_agro_year(%s, %s)
        """
        records = self.conn.extract_all(query, (agroid, year))

        fields = []
        for record in records:
            field_obj = Field(**record)
            fields.append(field_obj)
        return fields

    def get_bounds_lats_lons(
            self, dstype: int, agroid: int = None, fieldid: int = None,
            year: int = datetime.now().year
    ) -> Tuple[float, float, float, float]:
        """
        Возвращает координаты прямоугольника, охватывающего все поля
        во всех или одном из агропредприятий, или одно конкретное поле.
        Геометрия полей актуальна для переданного года.
        :param dstype: Идентификатор системы пространственной привязки (SRID)
                       для перепроецирования изображений.
        :param agroid: ID агропредприятия.
        :param fieldid: ID поля.
        :param year: Необходимый год.
        :return: Список координат охвата геометрии выбранных полей
        """
        query = """
        SELECT * FROM gpgeo.__geostl_get_boundpoints(%s, %s, %s, %s)
        """
        lats_lons = self.conn.extract_all(
            query=query, _vars=(year, dstype, fieldid, agroid)
        )
        return (
            lats_lons[0][0], lats_lons[0][1], lats_lons[2][0], lats_lons[2][1]
        )

    def save_field_geojson(
            self, fieldid: int, fieldname: int or str, date: str,
            agroid: int, dst_path: str, year=datetime.now().year,
    ) -> None:
        """
        Сохраняет в файл A#_FIELD#.geojson контур выбранного поля.
        :param fieldid: ID поля.
        :param fieldname: Номер поля.
        :param agroid: ID агропредприятия.
        :param dst_path: Выходной путь.
        :param year: Год, по которому выбирается контур.
        :param date: Дата, за которую формируется поле.
        """
        query = """SELECT * FROM gpgeo.__geo_get_field_shape(%s, %s)"""
        geojson = self.conn.extract_all(query, (fieldid, year))
        field_path = os.path.join(
            dst_path, f"A{agroid}_{date}_FIELD{fieldname}.geojson"
        )

        directory = os.path.dirname(field_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(os.path.join(field_path), "w+") as file:
            gj.dump(geojson[0][0], file)
        return

    def insert_ndvi_data(self, ndvi_values: List[NdviValues]) -> None:
        """
        Сохраняет данные в таблицу gpgeo.maps_ndvi_values.
        :param ndvi_values: Объекты данных таблицы NdviValues.
        """
        ndvi_values_tuples = []
        for item in ndvi_values:
            # Указываем порядок полей вручную
            data_tuple = (
                item.date,
                item.fieldid,
                item.ndvimean,
                item.ndvimax,
                item.ndvimin,
                None,
                item.ndvi_cv,
                item.is_uniform,
            )
            ndvi_values_tuples.append(data_tuple)

            self.conn.save_all(
                table=NdviValues, data=ndvi_values_tuples,
                on_conflict_fields="date, fieldid"
            )

    def insert_layer(self, layer: Layer) -> None:
        """Сохраняет данные в таблицу gpgeo.maps_layer."""
        if not self.check_layer(layer):
            self.conn.save_one(
                table=Layer, id_flag=False,
                _vars=(
                    layer.date,
                    None,
                    layer.set,
                    layer.resolution,
                    layer.agroid,
                    layer.name,
                    layer.satellite,
                    layer.isgrouplayer
                )
            )


def get_postgis_worker(pg_conn: _connection) -> PostgisWorker:
    """
    Возвращает экземпляр PostgisWorker.
    :param pg_conn: Соединение с postgis
    """
    connector = PostgisConnector(pg_conn)
    return PostgisWorker(connector)
