"""Оркестратор загрузки и поиска снимков."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

from tqdm import tqdm

from core.logging import get_logger
from core.settings import L1C_COLLECTION, L2A_COLLECTION
from .download import ODataProductDownloader
from .models import ProductRecord
from .search import ODataProductSearcher

logger = get_logger("CdseOrchestrator")


class SentinelDownloadOrchestrator:
    """Оркестратор поиска и скачивания."""

    def __init__(self, searcher: ODataProductSearcher,
                 downloader: ODataProductDownloader):
        self.searcher = searcher
        self.downloader = downloader

    def search(
            self,
            collection: str,
            start: str,
            end: str,
            do_download: bool,
            tiles: Optional[list[str]] = None,
            cloud_lt: Optional[float] = None,
            archive_index: Optional[set[str]] = None,
            product_type: Optional[str] = None,
    ) -> dict[str, list[ProductRecord]]:
        """
        Поиск с fallback L2A -> L1C.
        """
        items: List[ProductRecord] = self.searcher.search(
            collection=collection,
            start=start,
            end=end,
            do_download=do_download,
            tiles=tiles,
            cloud_lt=cloud_lt,
            product_type=product_type,
            archive_index=archive_index,
        )

        if not items and collection == L2A_COLLECTION:
            logger.info("L2A не найдено, переключаемся на L1C")
            items: List[ProductRecord] = self.searcher.search(
                collection=L1C_COLLECTION,
                start=start,
                end=end,
                do_download=do_download,
                tiles=tiles,
                cloud_lt=cloud_lt,
                product_type="S2MSI1C",
                archive_index=archive_index,
            )

        return self.group_by_tile(items, tiles)

    @staticmethod
    def group_by_tile(items: List[ProductRecord], tiles=None):
        """Группировка по тайлам."""
        grouped = {t.upper(): [] for t in tiles} if tiles else {}
        for item in items:
            tile = item.tile.upper() if item.tile else "-"
            grouped.setdefault(tile, []).append(item)
        for key in grouped:
            grouped[key].sort(key=lambda r: (r.date, r.name))
        return grouped

    def download(
            self,
            products,
            workers=1,
            archive_root="/mnt/map/Snapshots",
    ):
        """Параллельное скачивание архивов."""
        tasks = []
        for tile, items in products.items():
            if not items:
                continue

            for item in items:
                if item.exists:
                    continue

                tasks.append(item)

        if not tasks:
            logger.info("Нет архивов для скачивания")
            return

        logger.info("Запуск скачивания. Всего задач: %d", len(tasks))

        downloaded = 0
        failed = 0

        def _run(product: ProductRecord):
            return self.downloader.download_product(
                product=product,
                archive_root=archive_root,
            )

        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
            future_map = {executor.submit(_run, product): product for product
                          in tasks}

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
                        "Ошибка при обработке %s: %s",
                        product.name, exc
                    )

        logger.info(
            "Скачивание завершено. Успешно: %d, Ошибок: %d, Всего: %d",
            downloaded, failed, len(tasks)
        )
