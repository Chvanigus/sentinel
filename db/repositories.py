"""Repositories предметных данных Sentinel."""
from __future__ import annotations

from datetime import date
from typing import Any

from domain.models import Field, NdviStatistics, PublishedLayer

from .gateway import SqlGateway
from .models import LayerRecord, NdviRecord


class LayerRepository:
    """Чтение и сохранение опубликованных слоёв."""

    REQUIRED_SETS = frozenset({"ndvi", "ndwi", "tci"})
    REQUIRED_AGROIDS = (1, 3, 4, 5, 6)

    def __init__(self, gateway: SqlGateway):
        self.gateway = gateway

    def exists(self, layer: PublishedLayer) -> bool:
        """Проверяет наличие слоя с теми же датой, хозяйством и типом."""
        query = """
            SELECT 1
            FROM gpgeo.maps_layer
            WHERE date = %s AND agroid = %s AND set = %s
            LIMIT 1
        """
        return self.gateway.row(
            query,
            (layer.acquired_on, layer.agroid, layer.product),
        ) is not None

    def add(self, layer: PublishedLayer) -> None:
        """Сохраняет слой, если он ещё не зарегистрирован."""
        if not self.exists(layer):
            self.gateway.insert_one(
                LayerRecord,
                LayerRecord(
                    date=layer.acquired_on,
                    set=layer.product,
                    agroid=layer.agroid,
                    name=layer.name,
                ),
                conflict_fields="name",
            )

    def missing_agroids(self, acquired_on: date) -> list[int]:
        """Возвращает хозяйства без полного набора обязательных слоёв."""
        query = """
            SELECT DISTINCT agroid, set
            FROM gpgeo.maps_layer
            WHERE date = %s AND agroid = ANY (%s)
        """
        rows = self.gateway.rows(
            query,
            (acquired_on, list(self.REQUIRED_AGROIDS)),
        )
        found: dict[int, set[str]] = {}
        for row in rows:
            found.setdefault(row["agroid"], set()).add(row["set"])
        return [
            agroid
            for agroid in self.REQUIRED_AGROIDS
            if not self.REQUIRED_SETS.issubset(found.get(agroid, set()))
        ]


class FieldRepository:
    """Доступ к полям и их геометрии."""

    def __init__(self, gateway: SqlGateway):
        self.gateway = gateway

    def list_for_agro(self, agroid: int, year: int) -> list[Field]:
        """Возвращает поля хозяйства для указанного сезона."""
        rows = self.gateway.rows(
            "SELECT * FROM gpgeo.__geo_get_fieldnames_for_agro_year(%s, %s)",
            (agroid, year),
        )
        return [
            Field(id=int(row["id"]), name=str(row["name"]))
            for row in rows
        ]

    def bounds(
            self,
            srid: int,
            year: int,
            agroid: int | None = None,
            field_id: int | None = None,
    ) -> tuple[float, float, float, float]:
        """Возвращает прямоугольные границы хозяйства или отдельного поля."""
        rows = self.gateway.rows(
            "SELECT * FROM gpgeo.__geostl_get_boundpoints(%s, %s, %s, %s)",
            (year, srid, field_id, agroid),
        )
        if len(rows) < 3:
            raise LookupError(
                f"Не найдены границы: agroid={agroid}, field={field_id}"
            )
        return rows[0][0], rows[0][1], rows[2][0], rows[2][1]

    def geometries(
            self,
            field_ids: list[int],
            year: int,
    ) -> dict[int, Any]:
        """Возвращает геометрии полей в одном repository scope."""
        result = {}
        for field_id in field_ids:
            row = self.gateway.row(
                "SELECT * FROM gpgeo.__geo_get_field_shape(%s, %s)",
                (field_id, year),
            )
            if row is None:
                raise LookupError(f"Не найдена геометрия поля {field_id}")
            result[field_id] = row[0]
        return result


class NdviRepository:
    """Хранение статистики NDVI."""

    def __init__(self, gateway: SqlGateway):
        self.gateway = gateway

    def add_many(self, values: list[NdviStatistics]) -> None:
        """Сохраняет пакет доменных значений статистики NDVI."""
        rows = [
            (
                item.acquired_on,
                item.field_id,
                item.mean,
                item.maximum,
                item.minimum,
                None,
                item.coefficient_of_variation,
                item.is_uniform,
            )
            for item in values
        ]
        self.gateway.insert_many(
            NdviRecord,
            rows,
            conflict_fields="date, fieldid",
        )

    def is_complete(
            self,
            fields: list[Field],
            acquired_on: date,
    ) -> bool:
        """Проверяет наличие статистики за дату для каждого переданного поля."""
        field_ids = [field.id for field in fields if field.id is not None]
        if not field_ids:
            return False
        rows = self.gateway.rows(
            """
            SELECT DISTINCT fieldid
            FROM gpgeo.maps_ndvi_values
            WHERE date = %s AND fieldid = ANY (%s)
            """,
            (acquired_on, field_ids),
        )
        existing = {row["fieldid"] for row in rows}
        return set(field_ids).issubset(existing)
