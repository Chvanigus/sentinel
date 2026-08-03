"""Класс для работы с поиском данных в CDSE."""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from core.logging import get_logger

from .client import CdseODataClient
from .models import ProductRecord
from .utils import (
    extract_tile_from_item,
    normalize_tile,
    split_date_range,
)

logger = get_logger("CdseSearcher")


def _quote(value: str) -> str:
    """Экранирует строковое значение для OData-фильтра."""
    return value.replace("'", "''")


def _extract_cloud_cover(item: dict[str, Any]) -> float | None:
    """Извлекает процент облачности из типизированных атрибутов продукта."""
    attrs = item.get("Attributes") or []
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        if attr.get("Name") == "cloudCover":
            try:
                return float(attr.get("Value"))
            except (TypeError, ValueError) as exc:
                logger.warning("failed to parse cloudCover: %s", exc)
                return None
    return None


def _extract_size_bytes(item: dict[str, Any]) -> int | None:
    """Извлекает размер продукта из одного из поддерживаемых полей ответа."""
    value = item.get("ContentLength")
    if value is None:
        value = item.get("contentLength")
    if value is None:
        value = item.get("Size")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        logger.warning("failed parsing ContentLength: %s", exc)
        return None


class ODataProductSearcher:
    """
    Поиск продуктов через CDSE OData.
    """

    def __init__(
            self,
            client: CdseODataClient,
            chunk_days: int = 1,
    ):
        self.client = client
        self.chunk_days = chunk_days

    def _build_filter(
            self,
            collection: str,
            start_iso: str,
            end_iso: str,
            tiles: list[str] | None = None,
            cloud_lt: float | None = None,
            product_type: str | None = None,
    ) -> str:
        """Построение фильтра для поиска нужных продуктов."""
        filters: list[str] = [
            f"Collection/Name eq '{_quote(collection)}'",
            f"ContentDate/Start ge {start_iso}",
            f"ContentDate/Start lt {end_iso}",
        ]

        if product_type:
            filters.append(
                "Attributes/OData.CSC.StringAttribute/any("
                "att:att/Name eq 'productType' and "
                f"att/OData.CSC.StringAttribute/Value eq '{_quote(product_type)}')"
            )

        if tiles:
            normalized_tiles = [
                normalized
                for tile in tiles
                if (normalized := normalize_tile(tile))
            ]
            tile_filter = " or ".join(
                "Attributes/OData.CSC.StringAttribute/any("
                "att:att/Name eq 'tileId' and "
                f"att/OData.CSC.StringAttribute/Value eq '{_quote(tile)}')"
                for tile in normalized_tiles
            )
            if tile_filter:
                filters.append(f"({tile_filter})")

        if cloud_lt is not None:
            filters.append(
                "Attributes/OData.CSC.DoubleAttribute/any("
                "att:att/Name eq 'cloudCover' and "
                f"att/OData.CSC.DoubleAttribute/Value lt {float(cloud_lt):.2f})"
            )

        return " and ".join(filters)

    def iter_search(
            self,
            collection: str,
            start: str,
            end: str,
            tiles: list[str] | None = None,
            cloud_lt: float | None = None,
            product_type: str | None = None,
            archive_index: set[str] | None = None,
            top: int = 500,
            orderby: str | None = None,
    ) -> Iterator[ProductRecord]:
        """
        Генератор ProductRecord с дневным батчингом.
        """
        logger.info(
            "Поиск CDSE OData: collection=%s start=%s end=%s",
            collection, start, end
        )

        day_ranges = split_date_range(
            start,
            end,
            chunk_days=self.chunk_days,
        )

        for start_iso, end_iso in day_ranges:
            filter_expr = self._build_filter(
                collection=collection,
                start_iso=start_iso,
                end_iso=end_iso,
                tiles=tiles,
                cloud_lt=cloud_lt,
                product_type=product_type,
            )

            for item in self.client.iter_products(
                    filter_expr=filter_expr,
                    top=top,
                    orderby=orderby,
                    expand=["Attributes"],
                    authorized=True,
            ):
                if not isinstance(item, dict):
                    continue

                name = str(item.get("Name") or "").strip()
                product_id = str(item.get("Id") or "").strip()
                if not product_id or not name:
                    continue

                tile = extract_tile_from_item(item)
                date_value = ""
                content_date = item.get("ContentDate") or {}
                if isinstance(content_date, dict):
                    date_value = str(content_date.get("Start") or "")[:10]
                if not date_value:
                    dt = item.get("Created") or item.get("ModificationDate")
                    if isinstance(dt, str):
                        date_value = dt[:10]
                if not date_value:
                    continue

                size_bytes = _extract_size_bytes(item)
                cloud_cover = _extract_cloud_cover(item)

                archive_stem = (
                    name[:-5] if name.upper().endswith(".SAFE") else name
                )
                zip_name = f"{archive_stem}.zip"
                exists = archive_index is not None and zip_name in archive_index

                yield ProductRecord(
                    product_id=product_id,
                    name=name,
                    tile=tile or "-",
                    date=date_value,
                    cloud_cover=cloud_cover,
                    size_bytes=size_bytes,
                    exists=exists,
                    raw=item,
                )

    def search(
            self,
            collection: str,
            start: str,
            end: str,
            tiles: list[str] | None = None,
            cloud_lt: float | None = None,
            product_type: str | None = None,
            archive_index: set[str] | None = None,
            top: int = 500,
            orderby: str | None = None,
    ) -> list[ProductRecord]:
        """
        Возвращает список найденных продуктов.
        """
        records = list(
            self.iter_search(
                collection=collection,
                start=start,
                end=end,
                tiles=tiles,
                cloud_lt=cloud_lt,
                product_type=product_type,
                archive_index=archive_index,
                top=top,
                orderby=orderby,
            )
        )

        records.sort(key=lambda r: (r.tile or "", r.date or "", r.name or ""))
        return records
