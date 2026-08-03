"""Предметные сущности, не зависящие от БД и внешних API."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class Field:
    """Поле агропредприятия."""

    id: int
    name: str


@dataclass(frozen=True)
class NdviStatistics:
    """Статистика NDVI одного поля за дату."""

    acquired_on: date
    field_id: int
    mean: float | None
    maximum: float | None
    minimum: float | None
    growth_percent: float | None
    coefficient_of_variation: float | None
    is_uniform: bool
    valid_pixel_count: int | None = None
    total_pixel_count: int | None = None
    cloud_pixel_count: int | None = None
    nodata_pixel_count: int | None = None
    shadow_pixel_count: int | None = None
    snow_pixel_count: int | None = None
    valid_coverage_percent: float | None = None
    cloud_coverage_percent: float | None = None
    standard_deviation: float | None = None
    median: float | None = None
    percentile_10: float | None = None
    percentile_90: float | None = None
    source_level: str | None = None
    algorithm_version: str | None = None
    calculated_at: datetime | None = None


@dataclass(frozen=True)
class PublishedLayer:
    """Опубликованный геопространственный слой."""

    name: str
    acquired_on: date
    product: str
    agroid: int
