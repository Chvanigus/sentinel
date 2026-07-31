"""Низкоуровневый SQL gateway без доменной логики."""
from __future__ import annotations

import dataclasses
import re
from typing import Any

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor, execute_batch

from core.logging import get_logger

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SqlGateway:
    """Инкапсулирует cursor, транзакции и generic batch insert."""

    def __init__(self, pg_connection: connection) -> None:
        self.connection = pg_connection
        self.cursor = self.connection.cursor(cursor_factory=DictCursor)
        self.logger = get_logger(self.__class__.__name__)

    def __enter__(self) -> SqlGateway:
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Закрывает собственный cursor, но не внешнее подключение."""
        if self.cursor and not self.cursor.closed:
            self.cursor.close()

    @staticmethod
    def _field_names(record_type: type[Any]) -> list[str]:
        """Возвращает имена полей dataclass-модели."""
        if not dataclasses.is_dataclass(record_type):
            raise TypeError("record_type должен быть dataclass")
        return [field.name for field in dataclasses.fields(record_type)]

    def _insertable_fields(
            self,
            record_type: type[Any],
            include_id: bool = False,
    ) -> list[str]:
        """Возвращает поля вставки с опциональным первичным ключом."""
        fields = self._field_names(record_type)
        return fields if include_id else [
            field for field in fields if field != "id"
        ]

    @staticmethod
    def _table_name(record_type: type[Any]) -> str:
        """Проверяет и возвращает безопасное имя таблицы модели."""
        name = record_type.table_name()
        if not _IDENTIFIER.fullmatch(name):
            raise ValueError(f"Некорректное имя таблицы: {name}")
        return name

    @staticmethod
    def _conflict_columns(value: str) -> str:
        """Проверяет и экранирует столбцы условия ``ON CONFLICT``."""
        columns = [part.strip() for part in value.split(",")]
        if not columns or any(
                not _IDENTIFIER.fullmatch(column) for column in columns
        ):
            raise ValueError(f"Некорректные поля ON CONFLICT: {value}")
        return ", ".join(f'"{column}"' for column in columns)

    def _insert_query(
            self,
            record_type: type[Any],
            include_id: bool = False,
            conflict_fields: str = "id",
    ) -> str:
        """Строит параметризованный запрос вставки для dataclass-модели."""
        fields = self._insertable_fields(record_type, include_id)
        placeholders = ", ".join(["%s"] * len(fields))
        columns = ", ".join(f'"{field}"' for field in fields)
        table = self._table_name(record_type)
        conflict = self._conflict_columns(conflict_fields)
        return (
            f'INSERT INTO "gpgeo"."{table}" ({columns}) '
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict}) DO NOTHING;"
        )

    def rows(
            self,
            query: str,
            params: tuple[Any, ...] | None = None,
    ) -> list[dict]:
        """Выполняет запрос и возвращает все строки."""
        try:
            self.cursor.execute(query, params)
            return list(self.cursor.fetchall())
        except psycopg2.Error:
            self.logger.exception("Ошибка выполнения SQL-запроса")
            raise

    def row(
            self,
            query: str,
            params: tuple[Any, ...] | None = None,
    ) -> dict | None:
        """Выполняет запрос и возвращает одну строку либо ``None``."""
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchone()
        except psycopg2.Error:
            self.logger.exception("Ошибка выполнения SQL-запроса")
            raise

    def execute(
            self,
            query: str,
            params: tuple[Any, ...] | None = None,
            *,
            commit: bool = True,
    ) -> None:
        """Выполняет изменяющий запрос с управляемой фиксацией транзакции."""
        try:
            self.cursor.execute(query, params)
            if commit:
                self.connection.commit()
        except psycopg2.Error:
            self.connection.rollback()
            self.logger.exception("Ошибка изменяющего SQL-запроса")
            raise

    def tuples_for_insert(
            self,
            record_type: type[Any],
            records: list[Any],
            include_id: bool = False,
    ) -> list[tuple[Any, ...]]:
        """Преобразует модели или кортежи в значения для пакетной вставки."""
        fields = self._insertable_fields(record_type, include_id)
        result = []
        try:
            for record in records:
                result.append(
                    record if isinstance(record, tuple) else tuple(
                        getattr(record, field) for field in fields
                    )
                )
        except AttributeError as exc:
            raise TypeError(
                f"Ожидались экземпляры {record_type.__name__} или tuple"
            ) from exc
        return result

    def insert_one(
            self,
            record_type: type[Any],
            record: Any | tuple[Any, ...],
            include_id: bool = False,
            conflict_fields: str = "id",
    ) -> None:
        """Атомарно вставляет одну запись и откатывает ошибочную транзакцию."""
        values = self.tuples_for_insert(
            record_type,
            [record],
            include_id,
        )[0]
        expected = self._insertable_fields(record_type, include_id)
        if len(values) != len(expected):
            raise ValueError(
                f"Ожидалось {len(expected)} значений, получено {len(values)}"
            )
        query = self._insert_query(
            record_type,
            include_id,
            conflict_fields,
        )
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except psycopg2.Error:
            self.connection.rollback()
            self.logger.exception("Ошибка одиночной вставки")
            raise

    def insert_many(
            self,
            record_type: type[Any],
            records: list[Any],
            include_id: bool = False,
            conflict_fields: str = "id",
    ) -> None:
        """Атомарно вставляет пакет записей."""
        if not records:
            return
        values = self.tuples_for_insert(
            record_type,
            records,
            include_id,
        )
        query = self._insert_query(
            record_type,
            include_id,
            conflict_fields,
        )
        try:
            execute_batch(self.cursor, query, values, page_size=100)
            self.connection.commit()
        except psycopg2.Error:
            self.connection.rollback()
            self.logger.exception("Ошибка пакетной вставки")
            raise
