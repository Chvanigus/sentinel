"""Модели данных, передаваемые между domain и persistence слоями."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar


@dataclass
class DatabaseRecord:
    """Базовая модель строки с генерируемым первичным ключом."""

    # Генерируемый первичный ключ строки.
    id: int | None = None
    # Имя таблицы в схеме gpgeo; не является столбцом.
    table: ClassVar[str]

    @classmethod
    def table_name(cls) -> str:
        """Возвращает имя таблицы, связанной с persistence-моделью."""
        return cls.table


@dataclass
class LayerRecord(DatabaseRecord):
    """Строка ``maps_layer`` с метаданными слоя для интерактивной карты."""

    # Календарная дата съёмки.
    date: date | None = None
    # Поле либо NULL для слоя хозяйства.
    fieldid: int | None = None
    # Тип визуального слоя.
    set: str = ""
    # Идентификатор хозяйства.
    agroid: int = 0
    # Уникальное имя ресурса GeoServer.
    name: str = ""
    # Точное время начала съёмки в UTC.
    acquired_at: datetime | None = None
    # Спутник Sentinel-2.
    satellite: str | None = None
    # Уровень исходного продукта.
    source_level: str | None = None
    # Processing Baseline исходного продукта.
    processing_baseline: int | None = None
    # Тайлы, использованные для слоя.
    source_tiles: list[str] | None = None
    # Процент облачных пикселей по полям хозяйства.
    cloud_coverage_percent: float | None = None
    # Процент валидных пикселей по полям хозяйства.
    valid_coverage_percent: float | None = None
    # Разрешение растра в метрах.
    resolution_m: int | None = None
    # Применена ли облачная маска к визуальному растру.
    is_cloud_masked: bool = False
    # Версия алгоритма формирования слоя.
    algorithm_version: str | None = None
    # Время формирования слоя.
    generated_at: datetime | None = None
    table: ClassVar[str] = "maps_layer"


@dataclass
class NdviRecord(DatabaseRecord):
    """Persistence record таблицы ``maps_ndvi_values``."""

    # Календарная дата спутниковой съёмки.
    date: date | None = None
    # Точное время начала спутниковой съёмки в UTC.
    acquired_at: datetime | None = None
    # Поле, для которого рассчитана статистика.
    fieldid: int | None = None
    # Среднее NDVI по валидным пикселям.
    ndvimean: float | None = None
    # Максимальное NDVI по валидным пикселям.
    ndvimax: float | None = None
    # Минимальное NDVI по валидным пикселям.
    ndvimin: float | None = None
    # Процент изменения NDVI относительно предыдущего периода.
    growth_percent: float | None = None
    # Коэффициент вариации NDVI в процентах.
    ndvi_cv: float | None = None
    # Признак однородности валидной части поля.
    is_uniform: bool = False
    # Количество пикселей, вошедших в статистику.
    valid_pixel_count: int | None = None
    # Общее количество пикселей внутри поля.
    total_pixel_count: int | None = None
    # Количество облачных пикселей.
    cloud_pixel_count: int | None = None
    # Количество пикселей без данных.
    nodata_pixel_count: int | None = None
    # Количество пикселей тени.
    shadow_pixel_count: int | None = None
    # Количество пикселей снега или льда.
    snow_pixel_count: int | None = None
    # Доля валидных пикселей, проценты.
    valid_coverage_percent: float | None = None
    # Доля облачных пикселей, проценты.
    cloud_coverage_percent: float | None = None
    # Стандартное отклонение NDVI.
    ndvi_stddev: float | None = None
    # Медиана NDVI.
    ndvi_median: float | None = None
    # Десятый перцентиль NDVI.
    ndvi_p10: float | None = None
    # Девяностый перцентиль NDVI.
    ndvi_p90: float | None = None
    # Уровень обработки исходного продукта.
    source_level: str | None = None
    # Версия алгоритма расчёта.
    algorithm_version: str | None = None
    # Время фактического выполнения расчёта.
    calculated_at: datetime | None = None
    table: ClassVar[str] = "maps_ndvi_values"
