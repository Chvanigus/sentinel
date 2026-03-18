"""
HTTP клиент Copernicus Data Space Ecosystem (STAC).

Класс выполняет только сетевые операции:
- запросы STAC
- скачивание файлов
- автоматическое обновление access token при 401
"""

from __future__ import annotations

import os
import shutil
import socket
import time
from typing import List, Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.logging import get_logger
from core.settings.cdse import PAGE_LIMIT, STAC_BASE

logger = get_logger("Searcher")


class DataSpaceSearcher:
    """
    Поиск Sentinel-2 через STAC/CDSE.
    Отвечает только за поиск и возврат списка items.
    """

    def __init__(self, use_proxy: Optional[bool] = None):
        self.session = requests.Session()
        self.search_url = f"{STAC_BASE.rstrip('/')}/search"

        retry = Retry(
            total=5, connect=5, read=5, backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(
            max_retries=retry, pool_connections=20,
            pool_maxsize=20
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        if use_proxy is None:
            try:
                s = socket.socket()
                s.settimeout(0.5)
                s.connect(("127.0.0.1", 1080))
                s.close()
                use_proxy = True
            except Exception:
                use_proxy = False
        if use_proxy:
            self.session.proxies.update(
                {
                    "http": "socks5h://127.0.0.1:1080",
                    "https": "socks5h://127.0.0.1:1080"
                }
            )

        self.session.headers.update({
            "User-Agent": "sentinel-cdse-client",
            "Accept": "application/json",
            "Connection": "keep-alive",
        })

    def _request(self, method: str, url: str, retry_auth: bool = True,
                 **kwargs) -> requests.Response:
        """Обертка для requests с retry и обновлением токена."""
        kwargs.setdefault("timeout", 60)
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.request(method, url, **kwargs)
            except requests.RequestException as exc:
                logger.warning(
                    "Сетевая ошибка %s %s, попытка %d/%d: %s",
                    method, url, attempt, max_attempts, exc
                )
                if attempt < max_attempts:
                    time.sleep(2 ** (attempt - 1))
                    continue
                raise
            r.raise_for_status()
            return r
        raise RuntimeError("Не удалось выполнить сетевой запрос")

    @staticmethod
    def _normalize_date(start: str, end: str) -> str:
        """Приводим даты к ISO interval."""

        def norm(s, is_start=True):
            """Нормализация."""
            if any(x in s for x in ("T", "t", "_", " ")):
                return s
            return f"{s}T00:00:00Z" if is_start else f"{s}T23:59:59Z"

        return f"{norm(start)}/{norm(end, False)}"

    def search(self,
               collection: str,
               start: str,
               end: str,
               do_download: bool,
               tiles: Optional[List[str]] = None,
               cloud_lt: Optional[float] = None,
               archive_index=None) -> List[dict]:
        """Выполняет поиск и возвращает список dict items."""
        logger.info(
            "Поиск снимков в коллекции %s с %s по %s",
            collection,
            start, end
        )
        if do_download:
            logger.info(
                "Передан аргумент --download. "
                "После поиска будет запущен процесс скачивания "
                "выбранных снимков"
            )
        else:
            logger.info(
                "Поиск снимков будет происходить без скачивания, т.к. "
                "не был передан аргумент --download"
            )
        body = {
            "collections": [collection],
            "datetime": self._normalize_date(start, end),
            "limit": PAGE_LIMIT
        }
        filters = []
        if tiles:
            grid_tiles = [f"MGRS-{t}" for t in tiles]
            filters.append(
                {
                    "op": "in",
                    "args": [
                        {
                            "property": "grid:code"
                        },
                        grid_tiles
                    ]
                }
            )
        if cloud_lt is not None:
            filters.append(
                {
                    "op": "<",
                    "args": [
                        {
                            "property": "eo:cloud_cover"
                        },
                        cloud_lt
                    ]
                }
            )
        if filters:
            body["filter-lang"] = "cql2-json"
            body["filter"] = filters[0] if len(filters) == 1 else {"op": "and",
                                                                   "args": filters}

        r = self._request("POST", self.search_url, json=body)
        data = r.json()

        items = [i for i in data.get("features", []) if isinstance(i, dict)]

        self.summarize_items(items, archive_index)
        return items

    @staticmethod
    def _human_size(size_bytes: Optional[int]) -> str:
        """Человекочитаемая строка размера."""
        if not size_bytes:
            return "-"
        size = float(size_bytes)
        for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} ПБ"

    @staticmethod
    def _extract_tile(props: Dict[str, Any]) -> str:
        """Попытка получить тайл из properties (без префикса MGRS-)."""
        grid = props.get("grid:code") or props.get(
            "s2:mgrs_tile") or props.get("mgrs_tile")
        if not grid:
            title = props.get("title") or props.get("name")
            if title and isinstance(title, str):
                return (
                    title.split("_")[-1] if "_" in title else title).upper()
            return ""
        if isinstance(grid, str) and grid.upper().startswith("MGRS-"):
            return grid.upper().replace("MGRS-", "")
        return str(grid).upper()

    @staticmethod
    def _extract_product_size(item: Dict[str, Any]) -> Optional[int]:
        """
        Ищем размер продукта в нескольких местах:
         - properties['_private']['product_size']
         - assets[*].get('file:size') или assets[*].get('size')
        """
        props = item.get("properties", {}) or {}
        priv = props.get("_private", {}) or {}
        size = priv.get("product_size")
        if size:
            try:
                return int(size)
            except Exception as exc:
                logger.error(
                    f"Failed to parse product-size from private %s",
                    exc
                )
                pass

        # пробуем в assets - суммируем размеры (если есть)
        assets = item.get("assets", {}) or {}
        total = 0
        seen = False
        for a in assets.values():
            sz = None
            if isinstance(a, dict):
                sz = a.get("file:size") or a.get("size") or a.get("bytes")
            if sz:
                try:
                    total += int(sz)
                    seen = True
                except Exception as exc:
                    logger.debug(
                        f"Error parsing asset bytes '{sz}' ({exc}), skipping..."
                    )
                    continue
        if seen:
            return int(total)
        return None

    def summarize_items(self,
                        items: List[dict],
                        archive_index=None,
                        archive_base: str = "/mnt/map/Snapshots") -> List[Dict[str, Any]]:
        """
        Печатает красивую таблицу (сортированную по тайлу/дате)
        и возвращает список записей.
        """
        records: List[Dict[str, Any]] = []

        for item in items:
            if not isinstance(item, dict):
                continue

            props = item.get("properties", {}) or {}
            tile = self._extract_tile(props) or "-"
            date = (props.get("datetime") or "")[:10]
            cloud = props.get("eo:cloud_cover")
            size_bytes = self._extract_product_size(item)

            s3_links = [
                l for l in item.get("links", [])
                if l.get("rel") == "enclosure" and "s3://" in l.get("href", "")
            ]

            exists = False
            base_name = None

            if s3_links:
                s3_url = s3_links[0]["href"]
                base_name = os.path.basename(
                    s3_url.rstrip("/")
                ).replace(
                    ".SAFE", ""
                )

                if archive_index and base_name in archive_index:
                    exists = True

            records.append({
                "tile": tile,
                "date": date,
                "cloud": cloud,
                "size_bytes": size_bytes,
                "id": item.get("id"),
                "exists": exists,
                "base_name": base_name
            })

        records.sort(key=lambda r: (r["tile"] or "", r["date"] or ""))

        print()
        print(
            f"{'№ п/п':<5} | "
            f"{'Квадрат':<8} | "
            f"{'Дата':<10} | "
            f"{'Облачность %':<7} | "
            f"{'Размер':<10} | "
            f"{'Есть в архиве':<14} | "
            f"Полное название")
        print("-" * 137)
        i = 1
        for r in records:
            cloud_str = f"{r['cloud']:.2f}" if isinstance(r['cloud'], (int,
                                                                       float)) else "-"
            size_str = self._human_size(r["size_bytes"])
            exists_str = "✅" if r["exists"] else "❌"
            print(
                f"{i:>5} | "
                f"{r['tile']:<8} | "
                f"{r['date']:<10} | "
                f"{cloud_str:>12} | "
                f"{size_str:>10} | "
                f"{exists_str:<13} | "
                f"{r.get('id')}"
            )
            i += 1
        print()

        # === Общий размер (все снимки) ===
        total_size_bytes = sum(
            r["size_bytes"] for r in records
            if isinstance(r["size_bytes"], (int, float))
        )

        # === Только новые (к скачиванию) ===
        download_size_bytes = sum(
            r["size_bytes"] for r in records
            if not r["exists"] and isinstance(r["size_bytes"], (int, float))
        )

        # === Статистика ===
        total_count = len(records)
        exists_count = sum(1 for r in records if r["exists"])
        download_count = total_count - exists_count

        if download_count == 0:
            logger.info("🎉 Все снимки уже скачаны, загрузка не требуется")

        # === Свободное место на диске ===
        try:
            usage = shutil.disk_usage(archive_base)
            free_bytes = usage.free
            total_bytes = usage.total
            checked_path = archive_base
        except Exception as exc:
            logger.warning(
                "Не удалось получить информацию о диске для %s: %s. Будем использовать cwd.",
                archive_base, exc)
            usage = shutil.disk_usage(os.getcwd())
            free_bytes = usage.free
            total_bytes = usage.total
            checked_path = os.getcwd()

        total_size_str = self._human_size(total_size_bytes)
        download_size_str = self._human_size(download_size_bytes)
        free_str = self._human_size(free_bytes)
        disk_total_str = self._human_size(total_bytes)

        print(f"Информация проверена по пути: {checked_path}")

        print("=" * 137)
        print("Сводка по загрузке:")

        print(f"Всего найдено снимков        : {total_count}")
        print(f"Уже в архиве                 : {exists_count}")
        print(f"Будет скачано                : {download_count}")
        print("-" * 137)

        print(f"Общий размер снимков (всех)  : {total_size_str}")
        print(f"Размер к скачиванию          : {download_size_str}")
        print(f"Свободно на диске            : {free_str}")
        print(f"Всего на диске               : {disk_total_str}")

        if download_size_bytes > free_bytes:
            print("❌ Недостаточно места для скачивания на архиве!")
            deficit = download_size_bytes - free_bytes
            print(f"Не хватает: {self._human_size(deficit)}")
        else:
            print("✅ Места достаточно для скачивания на архиве")
            remaining = free_bytes - download_size_bytes
            print(f"Останется после загрузки: {self._human_size(remaining)}")

        print("=" * 137)
        return records
