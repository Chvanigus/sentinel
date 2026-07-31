"""Предметные сущности, не зависящие от БД и внешних API."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


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
    mean: float
    maximum: float
    minimum: float
    growth_percent: float
    coefficient_of_variation: float
    is_uniform: bool


@dataclass(frozen=True)
class PublishedLayer:
    """Опубликованный геопространственный слой."""

    name: str
    acquired_on: date
    product: str
    agroid: int
