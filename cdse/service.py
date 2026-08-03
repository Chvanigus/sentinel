"""Application service поиска и загрузки продуктов CDSE."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

from core.logging import get_logger

from .download import ODataProductDownloader
from .models import ProductRecord
from .search import ODataProductSearcher
from .selection import select_complete_acquisitions
from .utils import normalize_tile

logger = get_logger(__name__)


@dataclass(frozen=True)
class DownloadSummary:
    """Итог параллельной загрузки."""

    scheduled: int
    downloaded: int
    failed: int


class CdseService:
    """Use cases поиска с fallback и параллельной загрузки."""

    def __init__(
            self,
            searcher: ODataProductSearcher,
            downloader: ODataProductDownloader,
            fallback_collection: str,
            preferred_product_type: str,
            fallback_product_type: str,
    ):
        self.searcher = searcher
        self.downloader = downloader
        self.fallback_collection = fallback_collection
        self.preferred_product_type = preferred_product_type
        self.fallback_product_type = fallback_product_type

    def search(
            self,
            collection: str,
            start: str,
            end: str,
            tiles: list[str] | None = None,
            cloud_lt: float | None = None,
            archive_index: set[str] | None = None,
            product_type: str | None = None,
    ) -> dict[str, list[ProductRecord]]:
        """Ищет предпочтительный продукт и при отсутствии включает fallback."""
        preferred_items = self.searcher.search(
            collection=collection,
            start=start,
            end=end,
            tiles=tiles,
            cloud_lt=cloud_lt,
            product_type=product_type,
            archive_index=archive_index,
        )

        items = select_complete_acquisitions(preferred_items, tiles)
        selected_dates = {item.date for item in items}
        incomplete_dates = {
            item.date for item in preferred_items
        } - selected_dates

        if product_type == self.preferred_product_type and (
                not items or incomplete_dates
        ):
            logger.info(
                "Для %s нет полного комплекта за даты %s; ищем %s",
                self.preferred_product_type,
                ", ".join(sorted(incomplete_dates)) or "всего диапазона",
                self.fallback_product_type,
            )
            fallback_items = self.searcher.search(
                collection=self.fallback_collection,
                start=start,
                end=end,
                tiles=tiles,
                cloud_lt=cloud_lt,
                product_type=self.fallback_product_type,
                archive_index=archive_index,
            )
            items.extend(
                item
                for item in select_complete_acquisitions(
                    fallback_items,
                    tiles,
                )
                if item.date not in selected_dates
            )

        return self.group_by_tile(items, tiles)

    @staticmethod
    def group_by_tile(
            items: list[ProductRecord],
            tiles: list[str] | None = None,
    ) -> dict[str, list[ProductRecord]]:
        """Группирует продукты по нормализованному коду тайла."""
        grouped = (
            {normalize_tile(tile): [] for tile in tiles}
            if tiles
            else {}
        )
        for item in items:
            tile = normalize_tile(item.tile) or "-"
            grouped.setdefault(tile, []).append(item)
        for records in grouped.values():
            records.sort(key=lambda record: (record.date, record.name))
        return grouped

    def download(
            self,
            products: dict[str, list[ProductRecord]],
            archive_root: str | Path,
            workers: int = 1,
    ) -> DownloadSummary:
        """Параллельно скачивает отсутствующие архивы и возвращает итог."""
        tasks = [
            product
            for records in products.values()
            for product in records
            if not product.exists
        ]
        if not tasks:
            return DownloadSummary(0, 0, 0)

        downloaded = 0
        failed = 0
        with ThreadPoolExecutor(
                max_workers=max(1, int(workers))
        ) as executor:
            future_map = {
                executor.submit(
                    self.downloader.download_product,
                    product=product,
                    archive_root=archive_root,
                ): product
                for product in tasks
            }
            for future in tqdm(
                    as_completed(future_map),
                    total=len(future_map),
                    desc="Скачивание архивов",
                    unit="архив",
                    leave=True,
            ):
                product = future_map[future]
                try:
                    future.result()
                    downloaded += 1
                except Exception as exc:
                    failed += 1
                    logger.exception(
                        "Ошибка загрузки %s: %s",
                        product.name,
                        exc,
                    )

        summary = DownloadSummary(len(tasks), downloaded, failed)
        if failed:
            raise RuntimeError(
                f"Не удалось скачать архивов: {failed} из {len(tasks)}"
            )
        return summary
