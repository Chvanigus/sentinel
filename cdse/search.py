"""Класс для работы с поиском данных в CDSE."""
from __future__ import annotations

from typing import Any, Optional

from core.logging import get_logger
from core.settings import SEARCH_CHUNK_DAYS
from .client import CdseODataClient
from .models import ProductRecord
from .utils import (
    extract_tile_from_item,
    normalize_tile,
    split_date_range, )

logger = get_logger("CdseSearcher")


def _quote(value: str) -> str:
    return value.replace("'", "''")


def _extract_cloud_cover(item: dict[str, Any]) -> Optional[float]:
    attrs = item.get("Attributes") or []
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        if attr.get("Name") == "cloudCover":
            try:
                return float(attr.get("Value"))
            except Exception as exc:
                logger.warning("failed to parse cloudCover: %s", exc)
                return None
    return None


def _extract_size_bytes(item: dict[str, Any]) -> Optional[int]:
    value = item.get("ContentLength")
    if value is None:
        value = item.get("contentLength")
    if value is None:
        value = item.get("Size")
    if value is None:
        return None
    try:
        return int(value)
    except Exception as exc:
        logger.warning("failed parsing ContentLength: %s", exc)
        return None


class ODataProductSearcher:
    """
    Поиск продуктов через CDSE OData.
    """

    def __init__(self, client: CdseODataClient):
        self.client = client

    def _build_filter(
            self,
            collection: str,
            start_iso: str,
            end_iso: str,
            tiles: Optional[list[str]] = None,
            cloud_lt: Optional[float] = None,
            product_type: Optional[str] = None,
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
            normalized_tiles = [normalize_tile(t) for t in tiles if
                                normalize_tile(t)]
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
            do_download: bool,
            tiles: Optional[list[str]] = None,
            cloud_lt: Optional[float] = None,
            product_type: Optional[str] = None,
            archive_index: Optional[set[str]] = None,
            top: int = 500,
            orderby: Optional[str] = None,
    ):
        """
        Генератор ProductRecord с дневным батчингом.
        """
        logger.info(
            "Поиск CDSE OData: collection=%s start=%s end=%s",
            collection, start, end
        )

        if do_download:
            logger.info("После поиска будет запущено скачивание")
        else:
            logger.info("Поиск без скачивания")

        day_ranges = split_date_range(start, end, chunk_days=SEARCH_CHUNK_DAYS)

        for idx, (start_iso, end_iso) in enumerate(day_ranges, start=1):
            filter_expr = self._build_filter(
                collection=collection,
                start_iso=start_iso,
                end_iso=end_iso,
                tiles=tiles,
                cloud_lt=cloud_lt,
                product_type=product_type,
            )

            page_count = 0
            item_count = 0

            for item in self.client.iter_products(
                    filter_expr=filter_expr,
                    top=top,
                    orderby=orderby,
                    expand=["Attributes"],
                    authorized=True,
            ):
                page_count += 1
                item_count += 1

                if not isinstance(item, dict):
                    continue

                name = str(item.get("Name") or "").strip().replace(".SAFE", "")
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

                zip_name = f"{name}.zip"
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
            do_download: bool,
            tiles: Optional[list[str]] = None,
            cloud_lt: Optional[float] = None,
            product_type: Optional[str] = None,
            archive_index: Optional[set[str]] = None,
            top: int = 500,
            orderby: Optional[str] = None,
    ) -> list[ProductRecord]:
        """
        Возвращает список найденных продуктов.
        """
        records = list(
            self.iter_search(
                collection=collection,
                start=start,
                end=end,
                do_download=do_download,
                tiles=tiles,
                cloud_lt=cloud_lt,
                product_type=product_type,
                archive_index=archive_index,
                top=top,
                orderby=orderby,
            )
        )

        records = self._deduplicate_largest_per_tile_date(records)
        records.sort(key=lambda r: (r.tile or "", r.date or "", r.name or ""))
        return records

    @staticmethod
    def _deduplicate_largest_per_tile_date(records: list[ProductRecord]) -> \
            list[ProductRecord]:
        """
        Для каждой пары (tile, date) оставляет самый большой продукт.
        """
        best: dict[tuple[str, str], ProductRecord] = {}

        for record in records:
            key = (record.tile, record.date)
            current = best.get(key)
            if current is None:
                best[key] = record
                continue

            current_size = current.size_bytes or 0
            new_size = record.size_bytes or 0
            if new_size > current_size:
                best[key] = record

        return list(best.values())
