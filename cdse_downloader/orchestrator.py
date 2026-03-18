"""Оркестратор загрузки и поиска снимков."""
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from core.logging import get_logger
from core.settings import L2A_COLLECTION, L1C_COLLECTION, get_archive_dir
from .downloader import S3Downloader
from .searching import DataSpaceSearcher

logger = get_logger("CDSEOrchestrator")


class SentinelDownloadOrchestrator:
    """Оркестратор поиска и скачивания SAFE в ZIP."""

    def __init__(self, searcher: DataSpaceSearcher,
                 downloader: S3Downloader):
        self.searcher = searcher
        self.downloader = downloader

    def search(self,
               collection: str,
               start: str,
               end: str,
               do_download: bool,
               tiles=None,
               cloud_lt=None,
               archive_index=None):
        """Поиск и группировка по тайлам."""
        items = self.searcher.search(
            collection, start, end, do_download, tiles, cloud_lt,
            archive_index=archive_index
        )
        if not items and collection == L2A_COLLECTION:
            logger.info("L2A не найдено, переключаемся на L1C")
            items = self.searcher.search(
                L1C_COLLECTION, start, end, do_download, tiles, cloud_lt,
                archive_index=archive_index
            )
        return self.group_by_tile(items, tiles)

    @staticmethod
    def _extract_tile(item):
        props = item.get("properties", {}) or {}
        grid = props.get("grid:code") or props.get("s2:mgrs_tile")
        if not grid:
            return None
        return str(grid).upper().replace("MGRS-", "")

    def group_by_tile(self, items, tiles=None):
        """Группировка по тайлам."""
        grouped = {t.upper(): [] for t in tiles} if tiles else {}
        for item in items:
            tile = self._extract_tile(item)
            if tile:
                grouped.setdefault(tile, []).append(item)
        return grouped

    def download(self,
                 products,
                 workers=1,
                 per_archive_workers=1,
                 archive_index=None):
        """Параллельное скачивание SAFE в ZIP (архивы пишем сразу в целевой каталог)."""
        tasks = []
        for tile, items in products.items():
            if not items:
                continue
            for item in items:
                s3_links = [
                    l for l in item.get("links", [])
                    if l.get("rel") == "enclosure" and "s3://" in l.get("href",
                                                                        "")
                ]
                if not s3_links:
                    logger.warning("Нет S3 ссылки для тайла %s", tile)
                    continue
                s3_url = s3_links[0]["href"]
                base_name = os.path.basename(s3_url.rstrip("/"))
                base_name_no_safe = base_name.replace(".SAFE", "")

                if archive_index and base_name_no_safe in archive_index:
                    continue

                props = item.get("properties", {}) or {}
                date = (props.get("datetime") or "")[:10]
                year = date[:4] if date else "unknown"

                # сокращаем тайл (убираем первые 2 цифры зоны)
                tile_short = tile[2:] if tile and len(tile) > 2 and tile[
                    :2].isdigit() else tile

                # целевая папка архива
                archive_dir = get_archive_dir(year, tile_short)
                os.makedirs(archive_dir, exist_ok=True)

                local_dir = os.path.join(archive_dir, base_name)
                zip_name = base_name_no_safe + ".zip"
                zip_path = os.path.join(archive_dir, zip_name)

                if os.path.exists(zip_path):
                    logger.info("⏭ Уже существует: %s", zip_path)
                    continue

                tasks.append((s3_url, local_dir, zip_path, base_name_no_safe))

        if not tasks:
            logger.info("Нет архивов для скачивания")
            return

        logger.info("Запуск скачивания. Всего задач: %d", len(tasks))

        def _dl(task_tuple):
            s3_url, local_dir, zip_path, base_name_no_safe = task_tuple
            name = os.path.basename(zip_path)
            idx = task_index_map[base_name_no_safe]
            pos = idx % max(1, workers)
            desc = name
            try:
                self.downloader.download_folder(
                    s3_url,
                    local_dir,
                    max_workers=per_archive_workers,
                    progress_position=pos,
                    progress_desc=desc
                )
                self.downloader.make_zip(local_dir, zip_path)
                logger.info("✅ Успешно: %s", name)
            except Exception as exc:
                logger.exception("❌ Ошибка при обработке %s: %s", name, exc)
                try:
                    shutil.rmtree(local_dir, ignore_errors=True)
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
                except Exception:
                    pass
                raise

        # создаём маппинг задач -> индекс для позиции
        task_index_map = {t[3]: i for i, t in enumerate(tasks)}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_dl, t) for t in tasks]
            success = 0
            failed = 0
            # внешний tqdm остаётся (показывает количество завершённых архивов)
            for future in tqdm(as_completed(futures), total=len(futures),
                               desc="Скачивание архивов", unit="архив",
                               leave=True):
                try:
                    future.result()
                    success += 1
                except Exception:
                    failed += 1

        logger.info(
            "Скачивание архивов завершено. Успешно: %d, Ошибок: %d, Всего: %d",
            success, failed, len(tasks)
        )