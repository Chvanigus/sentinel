"""Модели данных, передаваемые между domain и persistence слоями."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
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
    ndvimean: float | None = None
    ndvimax: float | None = None
    ndvimin: float | None = None
    growth_percent: float | None = None
    ndvi_cv: float | None = None
    is_uniform: bool = False
    valid_pixel_count: int | None = None
    total_pixel_count: int | None = None
    cloud_pixel_count: int | None = None
    nodata_pixel_count: int | None = None
    shadow_pixel_count: int | None = None
    snow_pixel_count: int | None = None
    valid_coverage_percent: float | None = None
    cloud_coverage_percent: float | None = None
    ndvi_stddev: float | None = None
    ndvi_median: float | None = None
    ndvi_p10: float | None = None
    ndvi_p90: float | None = None
    source_level: str | None = None
    algorithm_version: str | None = None
    calculated_at: datetime | None = None
    table: ClassVar[str] = "maps_ndvi_values"
