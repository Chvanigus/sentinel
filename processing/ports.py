"""Порты processing domain к внешним системам."""
from __future__ import annotations

from datetime import date
from typing import Any, Protocol

from domain.models import Field, NdviStatistics


class FieldDataProvider(Protocol):
    """Данные полей и статистики без привязки к драйверу БД."""

    def bounds(
            self,
            *,
            year: int,
            agroid: int,
            srid: int,
    ) -> tuple[float, float, float, float]:
        """Возвращает границы хозяйства в заданной системе координат."""
        ...

    def fields(self, *, agroid: int, year: int) -> list[Field]:
        """Возвращает поля хозяйства за сезон."""
        ...

    def geometries(
            self,
            *,
            field_ids: list[int],
            year: int,
    ) -> dict[int, Any]:
        """Возвращает геометрии полей одним инфраструктурным вызовом."""
        ...

    def ndvi_is_complete(
            self,
            *,
            agroid: int,
            year: int,
            acquired_on: date,
    ) -> bool:
        """Проверяет полноту статистики хозяйства за дату."""
        ...

    def save_ndvi(
            self,
            values: list[NdviStatistics],
            *,
            field_ids: list[int],
            acquired_on: date,
            overwrite: bool = False,
    ) -> None:
        """Сохраняет либо полностью заменяет статистику выбранных полей."""
        ...
