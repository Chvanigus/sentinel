"""Выбор согласованных комплектов продуктов Sentinel из результатов CDSE."""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from core.logging import get_logger

from .models import ProductRecord
from .utils import normalize_tile

logger = get_logger(__name__)

_PRODUCT_NAME = re.compile(
    r"^(?P<satellite>S[1-9][A-Z])_"
    r"(?P<level>MSIL[12][AC])_"
    r"(?P<acquired>\d{8}T\d{6}).*?"
    r"_T(?P<tile>\d{2}[A-Z]{3})(?:_|\.)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ProductIdentity:
    """Идентичность одного продукта без времени его повторной публикации."""

    satellite: str
    level: str
    acquired_at: datetime
    tile: str


def parse_product_identity(record: ProductRecord) -> ProductIdentity | None:
    """Извлекает спутник, уровень, время съёмки и тайл из имени продукта."""
    match = _PRODUCT_NAME.search(record.name.upper())
    if match is None:
        logger.warning("Не удалось разобрать имя продукта CDSE: %s", record.name)
        return None
    return ProductIdentity(
        satellite=match.group("satellite").lower(),
        level=match.group("level").lower(),
        acquired_at=datetime.strptime(
            match.group("acquired"),
            "%Y%m%dT%H%M%S",
        ),
        tile=normalize_tile(match.group("tile")),
    )


def select_complete_acquisitions(
        records: list[ProductRecord],
        tiles: list[str] | None,
) -> list[ProductRecord]:
    """Выбирает за каждую дату крупнейший полный комплект одного пролёта."""
    requested_tiles = tuple(
        dict.fromkeys(
            normalized
            for tile in (tiles or [])
            if (normalized := normalize_tile(tile))
        )
    )
    if not requested_tiles:
        requested_tiles = tuple(sorted({normalize_tile(item.tile) for item in records}))
    if not requested_tiles:
        return []

    grouped: dict[
        tuple[str, str, datetime],
        dict[str, ProductRecord],
    ] = defaultdict(dict)
    for record in records:
        identity = parse_product_identity(record)
        if identity is None or identity.tile not in requested_tiles:
            continue
        key = (identity.satellite, identity.level, identity.acquired_at)
        current = grouped[key].get(identity.tile)
        if current is None or (record.size_bytes or 0) > (current.size_bytes or 0):
            grouped[key][identity.tile] = record

    complete_by_date: dict[str, list[tuple[int, tuple[str, ...], list[ProductRecord]]]] = \
        defaultdict(list)
    for (_satellite, _level, acquired_at), by_tile in grouped.items():
        if not all(tile in by_tile for tile in requested_tiles):
            continue
        selected = [by_tile[tile] for tile in requested_tiles]
        complete_by_date[acquired_at.date().isoformat()].append(
            (
                sum(item.size_bytes or 0 for item in selected),
                tuple(item.name for item in selected),
                selected,
            )
        )

    result = []
    for acquired_on in sorted(complete_by_date):
        _size, _names, selected = max(
            complete_by_date[acquired_on],
            key=lambda candidate: (candidate[0], candidate[1]),
        )
        result.extend(selected)
    return result
