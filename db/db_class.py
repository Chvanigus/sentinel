"""Диспетчер контекста для подключения и работы с базой данных."""
import dataclasses
import os
import sys
from datetime import datetime
from typing import List, Optional, Tuple

import geojson as gj
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch

from core.logging import get_logger
from db.data_class import NdviValues, Field, Layer


class PostgisConnector:
    """
    Класс для получения и сохранения данных из БД.
    """

    def __init__(self, pg_conn: _connection) -> None:
        self.con = pg_conn
        self.cur = self.con.cursor(cursor_factory=DictCursor)
        self.logger = get_logger("DBConnector")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur:
            self.cur.close()
        if self.con:
            self.con.close()

    @staticmethod
    def _get_field_names(table: dataclasses.dataclass) -> List[str]:
        """
        Возвращает список полей dataclass в определённом порядке.
        """
        return list(table.__dataclass_fields__.keys())

    def _get_insertable_fields(self,
                               table: dataclasses.dataclass,
                               id_flag: bool = False) -> List[str]:
        """
        Возвращает список колонок, которые будут вставляться (с учётом id_flag).
        """
        fields = self._get_field_names(table)
        if not id_flag and 'id' in fields:
            fields = [f for f in fields if f != 'id']
        return fields

    def _get_query_to_save(
            self, table, id_flag: bool = False,
            on_conflict_fields: str = "id"
    ) -> str:
        """
        Возвращает SQL запрос для сохранения данных в БД.
        Формат: INSERT INTO "gpgeo"."table" ("col1","col2",...)
        VALUES (%s,%s,...) ON CONFLICT (...) DO NOTHING;
        """
        insert_fields = self._get_insertable_fields(table, id_flag=id_flag)
        placeholders = ",".join(["%s"] * len(insert_fields))
        # Добавляем кавычки на имена колонок (без схемы)
        cols_sql = ", ".join([f'"{c}"' for c in insert_fields])

        query = (
            f'INSERT INTO "gpgeo"."{table.TableName()}" '
            f'({cols_sql}) VALUES ({placeholders}) '
            f'ON CONFLICT ({on_conflict_fields}) DO NOTHING;'
        )
        return query

    def get_tuples_data_for_save(
            self, table: dataclasses.dataclass, data: list
    ) -> Optional[List[tuple]]:
        """
        Возвращает список данных преобразованных в кортеж для сохранения в БД.
        Поддерживает список кортежей или список объектов dataclass с
        методом to_tuple() или .to_tuple эмитируется внешне.
        """
        tuple_data = []
        try:
            for it in data:
                if isinstance(it, tuple):
                    tuple_data.append(it)
                else:
                    # Если объект — dataclass, попробуем получить в порядке полей
                    if hasattr(it, "to_tuple"):
                        tuple_data.append(it.to_tuple())
                    else:
                        # Попробуем собрать tuple по полям dataclass автоматически
                        fields = list(it.__class__.__dataclass_fields__.keys())
                        values = tuple(getattr(it, f) for f in fields)
                        tuple_data.append(values)
            return tuple_data
        except AttributeError:
            self.logger.error(
                "Не удалось преобразовать данные в кортеж. "
                "Проверьте, что данные переданы в виде списка объектов "
                "класса %s или в виде списка кортежей.",
                table.__name__
            )
            return None

    def extract_all(self,
                    query: str,
                    _vars: tuple = None) -> Optional[List[dict]]:
        """Извлекает все данные из БД."""
        try:
            self.cur.execute(query, _vars)
            return self.cur.fetchall()
        except psycopg2.Error as e:
            self.logger.critical(
                "Ошибка при попытке выполнения запроса: %s", e
            )
            sys.exit(-1)

    def extract_one(self, query: str, _vars: tuple = None) -> Optional[dict]:
        """Извлекает одну запись из БД."""
        try:
            self.cur.execute(query, _vars)
            return self.cur.fetchone()
        except psycopg2.Error as e:
            self.logger.critical(
                "Ошибка при попытке выполнения запроса: %s", e
            )
            sys.exit(-1)

    def save_one(self,
                 table: dataclasses.dataclass,
                 _vars: dataclasses.dataclass or tuple,
                 id_flag: bool = False,
                 on_conflict_fields: str = "id") -> bool:
        """
        Сохранение одной записи.
        Проверяет, что количество переданных переменных соответствует
        ожидаемому числу колонок.
        """
        if isinstance(_vars, type) and dataclasses.is_dataclass(_vars):
            raise TypeError(
                "save_one: передан dataclass-класс, ожидается экземпляр или tuple. "
                "Используйте _vars=layer (объект) или _vars=(...tuple...)."
            )

        expected_fields = self._get_insertable_fields(table, id_flag=id_flag)

        if dataclasses.is_dataclass(_vars):
            _vars = tuple(getattr(_vars, f) for f in expected_fields)

        if len(_vars) != len(expected_fields):
            msg = (
                f"save_one: несоответствие количества переменных.\n"
                f"Ожидалось: {len(expected_fields)} {expected_fields}\n"
                f"Получено:  {len(_vars)} {_vars}"
            )
            self.logger.error(msg)
            self.con.rollback()
            raise ValueError(msg)

        query = self._get_query_to_save(
            table=table,
            id_flag=id_flag,
            on_conflict_fields=on_conflict_fields
        )

        try:
            self.cur.execute(query, _vars)
            self.con.commit()
            return True
        except psycopg2.Error as e:
            self.con.rollback()
            self.logger.error(
                f"Ошибка save_one: %s\nSQL: %s\nVARS: %s",
                e, query, _vars
            )
            raise

    def save_all(
            self, table: dataclasses.dataclass,
            data: List[dataclasses.dataclass] or List[Tuple],
            id_flag: bool = False,
            on_conflict_fields: str = "id"
    ) -> bool:
        """Сохранение всех записей."""
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
            try:
                self.con.rollback()
            except Exception as e:
                self.logger.error(
                    "Ошибка при откате транзакции: %s", e
                )
                pass
            self.logger.error(
                "Ошибка в save_all: %s; SQL: %s",
                err, query
            )
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
        """
        Сохраняет слой в таблицу gpgeo.maps_layer.
        """

        if self.check_layer(layer):
            return

        self.conn.save_one(
            table=Layer,
            _vars=layer,
            id_flag=False, on_conflict_fields="name"
        )

    def has_ndvi_records_for_agro(
            self, agroid: int, year: int, date_obj: datetime
    ) -> bool:
        """
        Проверяет: есть ли уже NDVI-записи в БД для этого агро и даты.
        """
        fields = self.get_fieldids_from_agro(agroid, year)
        if not fields:
            return False

        field_ids = [f.id for f in fields]

        query = """
        SELECT 1
        FROM gpgeo.maps_ndvi_values
        WHERE date = %s
          AND fieldid = ANY(%s)
        LIMIT 1
        """

        result = self.conn.extract_one(
            query=query,
            _vars=(date_obj, field_ids)
        )

        return result is not None


def get_postgis_worker(pg_conn: _connection) -> PostgisWorker:
    """
    Возвращает экземпляр PostgisWorker.
    :param pg_conn: Соединение с postgis
    """
    connector = PostgisConnector(pg_conn)
    return PostgisWorker(connector)
