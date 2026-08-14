"""Предметные сущности, не зависящие от БД и внешних API."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class Field:
    """Поле агропредприятия."""

    # Идентификатор поля в PostGIS.
    id: int
    # Пользовательское название поля.
    name: str
    # Код поля, используемый для внешнего выбора, например A3/F100б.
    fieldcode: str | None = None


@dataclass(frozen=True)
class NdviStatistics:
    """Статистика NDVI одного поля за дату."""

    # Календарная дата спутниковой съёмки.
    acquired_on: date
    # Поле, для которого рассчитана статистика.
    field_id: int
    # Среднее NDVI по валидным пикселям.
    mean: float | None
    # Максимальное NDVI по валидным пикселям.
    maximum: float | None
    # Минимальное NDVI по валидным пикселям.
    minimum: float | None
    # Процент изменения относительно выбранного предыдущего периода.
    growth_percent: float | None
    # Коэффициент вариации NDVI в процентах.
    coefficient_of_variation: float | None
    # Признак однородности валидной части поля.
    is_uniform: bool
    # Точное время начала спутниковой съёмки в UTC.
    acquired_at: datetime | None = None
    # Количество пикселей, вошедших в численную статистику.
    valid_pixel_count: int | None = None
    # Общее количество пикселей внутри геометрии поля.
    total_pixel_count: int | None = None
    # Количество облачных пикселей по SCL.
    cloud_pixel_count: int | None = None
    # Количество пикселей без данных или с некорректным NDVI.
    nodata_pixel_count: int | None = None
    # Количество пикселей тени по SCL.
    shadow_pixel_count: int | None = None
    # Количество пикселей снега или льда по SCL.
    snow_pixel_count: int | None = None
    # Доля валидных пикселей от площади поля, проценты.
    valid_coverage_percent: float | None = None
    # Доля облачных пикселей от площади поля, проценты.
    cloud_coverage_percent: float | None = None
    # Стандартное отклонение NDVI.
    standard_deviation: float | None = None
    # Медиана NDVI.
    median: float | None = None
    # Десятый перцентиль NDVI.
    percentile_10: float | None = None
    # Девяностый перцентиль NDVI.
    percentile_90: float | None = None
    # Уровень обработки исходного продукта Sentinel-2.
    source_level: str | None = None
    # Версия алгоритма расчёта.
    algorithm_version: str | None = None
    # Время фактического выполнения расчёта.
    calculated_at: datetime | None = None


@dataclass(frozen=True)
class PublishedLayer:
    """Опубликованный слой и метаданные, которые получает интерактивная карта."""

    # Полное уникальное имя ресурса GeoServer.
    name: str
    # Календарная дата спутниковой съёмки.
    acquired_on: date
    # Тип визуального слоя: tci, ndvi, ndwi или scl.
    product: str
    # Хозяйство, для территории которого подготовлен слой.
    agroid: int
    # Точное время начала спутниковой съёмки в UTC.
    acquired_at: datetime | None = None
    # Спутник-источник, например S2A или S2B.
    satellite: str | None = None
    # Уровень обработки исходного продукта: L1C или L2A.
    source_level: str | None = None
    # Processing Baseline исходного продукта Sentinel-2.
    processing_baseline: int | None = None
    # Тайлы Sentinel-2, из которых сформирован слой хозяйства.
    source_tiles: tuple[str, ...] = ()
    # Доля облачных пикселей по объединённой территории полей хозяйства.
    cloud_coverage_percent: float | None = None
    # Доля валидных пикселей по объединённой территории полей хозяйства.
    valid_coverage_percent: float | None = None
    # Пространственное разрешение растра в метрах на пиксель.
    resolution_m: int | None = None
    # Признак применения облачной маски к отображаемому растру.
    is_cloud_masked: bool = False
    # Версия алгоритма формирования слоя и его метаданных.
    algorithm_version: str | None = None
    # Время формирования либо последнего обновления слоя.
    generated_at: datetime | None = None


@dataclass(frozen=True)
class LayerSourceMetadata:
    """Общие метаданные исходной пары, передаваемые в публикацию слоёв."""

    # Точное время начала спутниковой съёмки в UTC.
    acquired_at: datetime
    # Спутник, выполнивший съёмку.
    satellite: str
    # Уровень обработки исходного продукта.
    source_level: str
    # Processing Baseline исходного продукта.
    processing_baseline: int | None
    # Исходные тайлы для каждого хозяйства.
    source_tiles_by_agroid: dict[int, tuple[str, ...]]
    # Версия алгоритма формирования визуальных слоёв.
    algorithm_version: str


@dataclass(frozen=True)
class LayerMetadataUpdate:
    """Метаданные существующих слоёв одного хозяйства за дату съёмки."""

    # Календарная дата спутниковой съёмки.
    acquired_on: date
    # Хозяйство, слои которого требуется обновить.
    agroid: int
    # Точное время начала съёмки в UTC.
    acquired_at: datetime
    # Спутник, выполнивший съёмку.
    satellite: str
    # Уровень обработки исходного продукта.
    source_level: str
    # Processing Baseline исходного продукта.
    processing_baseline: int | None
    # Исходные тайлы, покрывающие хозяйство.
    source_tiles: tuple[str, ...]
    # Версия-заглушка для слоя с неизвестным историческим алгоритмом.
    fallback_algorithm_version: str = "legacy"
    # Пространственное разрешение опубликованного растра в метрах.
    resolution_m: int = 10
    # Признак маскирования отображаемого растра облаками.
    is_cloud_masked: bool = False
