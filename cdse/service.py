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
from .utils import disk_usage, human_size, normalize_tile

logger = get_logger(__name__)


def _remaining_download_size(
        products: list[ProductRecord],
        archive_root: str | Path,
) -> int | None:
    """Оценивает оставшийся объём с учётом частично скачанных файлов."""
    root = Path(archive_root)
    remaining = 0
    for product in products:
        if product.size_bytes is None or product.size_bytes <= 0:
            return None
        temporary = (
            root
            / (product.date[:4] if product.date else "unknown")
            / (normalize_tile(product.tile) or "unknown")
            / f"{product.archive_name}.tmp"
        )
        downloaded = temporary.stat().st_size if temporary.is_file() else 0
        remaining += max(product.size_bytes - downloaded, 0)
    return remaining


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
            logger.info("Новых архивов для скачивания нет")
            return DownloadSummary(0, 0, 0)
        if workers < 1:
            raise ValueError("Число потоков скачивания должно быть положительным")

        downloaded = 0
        failed = 0
        sizes = [product.size_bytes for product in tasks]
        total_bytes = (
            sum(size for size in sizes if size is not None)
            if all(size is not None and size > 0 for size in sizes)
            else None
        )
        archive_path = Path(archive_root)
        archive_path.mkdir(parents=True, exist_ok=True)
        remaining_bytes = _remaining_download_size(tasks, archive_path)
        if remaining_bytes is not None:
            free_bytes, _total_disk_bytes = disk_usage(str(archive_path))
            if remaining_bytes > free_bytes:
                raise RuntimeError(
                    "Недостаточно места для скачивания: требуется "
                    f"{human_size(remaining_bytes)}, доступно "
                    f"{human_size(free_bytes)}"
                )
        logger.info(
            "Запланировано архивов: %s; потоков: %s; общий объём: %s; "
            "осталось получить: %s",
            len(tasks),
            workers,
            human_size(total_bytes) if total_bytes is not None else "неизвестен",
            (
                human_size(remaining_bytes)
                if remaining_bytes is not None
                else "неизвестно"
            ),
        )
        with tqdm(
                total=total_bytes,
                desc="Скачано",
                unit="Б",
                unit_scale=True,
                unit_divisor=1024,
                dynamic_ncols=True,
                mininterval=0.5,
                leave=True,
        ) as byte_progress:
            with ThreadPoolExecutor(max_workers=int(workers)) as executor:
                future_map = {
                    executor.submit(
                        self.downloader.download_product,
                        product=product,
                        archive_root=archive_root,
                        progress=byte_progress.update,
                    ): product
                    for product in tasks
                }
                for future in as_completed(future_map):
                    product = future_map[future]
                    try:
                        future.result()
                        downloaded += 1
                        byte_progress.set_postfix_str(
                            f"архивов {downloaded}/{len(tasks)}"
                        )
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
        logger.info(
            "Скачивание завершено: %s из %s архивов",
            downloaded,
            len(tasks),
        )
        return summary
