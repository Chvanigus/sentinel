"""PostGIS-реализации processing ports."""
from __future__ import annotations

from datetime import date
from typing import Any

import psycopg2

from db.connection import get_database_config
from db.gateway import SqlGateway
from db.repositories import FieldRepository, NdviRepository
from domain.models import Field, NdviStatistics


class PostgisFieldDataProvider:
    """Читает PostGIS короткими scope и кеширует сезонные справочники."""

    def __init__(self) -> None:
        self._bounds: dict[
            tuple[int, int, int],
            tuple[float, float, float, float],
        ] = {}
        self._fields: dict[tuple[int, int], list[Field]] = {}
        self._geometries: dict[tuple[int, int], Any] = {}

    def bounds(
            self,
            *,
            year: int,
            agroid: int,
            srid: int,
    ) -> tuple[float, float, float, float]:
        """Читает границы хозяйства в заданной системе координат."""
        key = (year, agroid, srid)
        if key in self._bounds:
            return self._bounds[key]
        with psycopg2.connect(**get_database_config()) as connection:
            value = FieldRepository(SqlGateway(connection)).bounds(
                srid=srid,
                year=year,
                agroid=agroid,
            )
        self._bounds[key] = value
        return value

    def fields(self, *, agroid: int, year: int) -> list[Field]:
        """Читает поля хозяйства за сезон."""
        key = (agroid, year)
        if key in self._fields:
            return self._fields[key]
        with psycopg2.connect(**get_database_config()) as connection:
            values = FieldRepository(SqlGateway(connection)).list_for_agro(
                agroid,
                year,
            )
        self._fields[key] = values
        return values

    def geometries(
            self,
            *,
            field_ids: list[int],
            year: int,
    ) -> dict[int, Any]:
        """Читает набор геометрий в одном connection scope."""
        missing = [
            field_id
            for field_id in field_ids
            if (year, field_id) not in self._geometries
        ]
        if missing:
            with psycopg2.connect(**get_database_config()) as connection:
                loaded = FieldRepository(
                    SqlGateway(connection)
                ).geometries(
                    missing,
                    year,
                )
            self._geometries.update(
                ((year, field_id), geometry)
                for field_id, geometry in loaded.items()
            )
        return {
            field_id: self._geometries[(year, field_id)]
            for field_id in field_ids
        }

    def ndvi_is_complete(
            self,
            *,
            agroid: int,
            year: int,
            acquired_on: date,
    ) -> bool:
        """Проверяет полноту NDVI-статистики хозяйства за дату."""
        fields = self.fields(agroid=agroid, year=year)
        with psycopg2.connect(**get_database_config()) as connection:
            gateway = SqlGateway(connection)
            return NdviRepository(gateway).is_complete(fields, acquired_on)

    def add_ndvi(self, values: list[NdviStatistics]) -> None:
        """Сохраняет рассчитанную статистику NDVI."""
        with psycopg2.connect(**get_database_config()) as connection:
            NdviRepository(SqlGateway(connection)).add_many(values)
