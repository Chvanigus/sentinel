"""Модели данных, передаваемые между domain и persistence слоями."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import ClassVar


@dataclass
class DatabaseRecord:
    """Базовая модель строки с генерируемым первичным ключом."""

    id: int | None = None
    table: ClassVar[str]

    @classmethod
    def table_name(cls) -> str:
        """Возвращает имя таблицы, связанной с persistence-моделью."""
        return cls.table


@dataclass
class LayerRecord(DatabaseRecord):
    """Persistence record таблицы ``maps_layer``."""

    date: date | None = None
    fieldid: int | None = None
    set: str = ""
    agroid: int = 0
    name: str = ""
    table: ClassVar[str] = "maps_layer"


@dataclass
class NdviRecord(DatabaseRecord):
    """Persistence record таблицы ``maps_ndvi_values``."""

    date: date | None = None
    fieldid: int | None = None
    ndvimean: float = 0.0
    ndvimax: float = 0.0
    ndvimin: float = 0.0
    growth_percent: float = 0.0
    ndvi_cv: float = 0.0
    is_uniform: bool = True
    table: ClassVar[str] = "maps_ndvi_values"
