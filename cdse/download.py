"""Модуль скачивания данных из CDSE."""
from __future__ import annotations

import os
import re
import time
import zipfile
from collections.abc import Callable
from pathlib import Path
from time import perf_counter

import requests

from core.logging import get_logger

from .client import CdseODataClient
from .exceptions import CdseDownloadError, CdseQueryError
from .models import ProductRecord
from .utils import ensure_dir, normalize_tile

logger = get_logger("CdseDownloader")

MAX_RETRIES = 5
CHUNK_SIZE = 1024 * 1024
PROGRESS_LOG_INTERVAL = 30.0
CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$")

ProgressCallback = Callable[[int], object]


def _format_size(size_bytes: int | None) -> str:
    """Форматирует размер файла в двоичных единицах."""
    if size_bytes is None:
        return "размер неизвестен"
    value = float(size_bytes)
    for unit in ("Б", "КиБ", "МиБ", "ГиБ", "ТиБ"):
        if value < 1024 or unit == "ТиБ":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} ТиБ"


def _response_total_size(response, offset: int) -> int | None:
    """Определяет полный размер файла из HTTP-заголовков ответа."""
    headers = getattr(response, "headers", {}) or {}
    content_range = headers.get("Content-Range")
    if content_range:
        match = CONTENT_RANGE_RE.match(content_range.strip())
        if match and match.group(3) != "*":
            return int(match.group(3))
    content_length = headers.get("Content-Length")
    if content_length:
        try:
            remaining = int(content_length)
        except (TypeError, ValueError):
            return None
        return offset + remaining if response.status_code == 206 else remaining
    return None


def _response_range_start(response) -> int | None:
    """Извлекает начальную позицию частичного HTTP-ответа."""
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("Content-Range")
    if not value:
        return None
    match = CONTENT_RANGE_RE.match(value.strip())
    return int(match.group(1)) if match else None


def _report_progress(
        callback: ProgressCallback | None,
        delta: int,
) -> None:
    """Передаёт изменение числа байт общему индикатору загрузки."""
    if callback is not None and delta:
        callback(delta)


class ODataProductDownloader:
    """
    Скачивание продукта CDSE и упаковка SAFE в ZIP для долгосрочного хранения.
    """

    def __init__(self, client: CdseODataClient):
        self.client = client

    def _download_with_resume(
            self,
            product_id: str,
            tmp_file: Path,
            *,
            label: str | None = None,
            expected_size: int | None = None,
            progress: ProgressCallback | None = None,
    ) -> tuple[int, int]:
        """Скачивает файл с докачкой и возвращает размер и сетевой трафик."""
        display_name = label or product_id
        reported_size = 0
        transferred = 0
        started = perf_counter()
        last_progress_log = started

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                downloaded = tmp_file.stat().st_size if tmp_file.exists() else 0
                _report_progress(progress, downloaded - reported_size)
                reported_size = downloaded

                headers = {"Accept-Encoding": "identity"}
                if downloaded > 0:
                    headers["Range"] = f"bytes={downloaded}-"
                    logger.info(
                        "Продолжаем загрузку %s с позиции %s",
                        display_name,
                        _format_size(downloaded),
                    )

                response = self.client.download_stream(
                    product_id, authorized=True, headers=headers
                )
                try:
                    response.raise_for_status()

                    if downloaded > 0 and response.status_code != 206:
                        logger.warning(
                            "CDSE не подтвердил докачку %s; начинаем файл заново",
                            display_name,
                        )
                        _report_progress(progress, -downloaded)
                        reported_size = 0
                        downloaded = 0
                    elif downloaded > 0:
                        range_start = _response_range_start(response)
                        if range_start is not None and range_start != downloaded:
                            raise CdseDownloadError(
                                "CDSE вернул неверную позицию докачки "
                                f"для {display_name}: {range_start} вместо "
                                f"{downloaded}"
                            )

                    mode = "ab" if downloaded > 0 else "wb"
                    response_total = _response_total_size(response, downloaded)
                    total_size = response_total or expected_size
                    with open(tmp_file, mode) as file_obj:
                        for chunk in response.iter_content(CHUNK_SIZE):
                            if chunk:
                                file_obj.write(chunk)
                                chunk_size = len(chunk)
                                downloaded += chunk_size
                                transferred += chunk_size
                                reported_size += chunk_size
                                _report_progress(progress, chunk_size)
                                now = perf_counter()
                                if now - last_progress_log >= PROGRESS_LOG_INTERVAL:
                                    elapsed = max(now - started, 0.001)
                                    speed = transferred / elapsed
                                    percent = (
                                        downloaded / total_size * 100
                                        if total_size
                                        else None
                                    )
                                    logger.info(
                                        "Загрузка %s: %s%s, скорость %s/с",
                                        display_name,
                                        _format_size(downloaded),
                                        (
                                            f" из {_format_size(total_size)} "
                                            f"({percent:.1f}%)"
                                            if percent is not None
                                            else ""
                                        ),
                                        _format_size(round(speed)),
                                    )
                                    last_progress_log = now
                    actual_size = tmp_file.stat().st_size
                    if response_total is not None and actual_size != response_total:
                        raise requests.exceptions.ChunkedEncodingError(
                            "Получено "
                            f"{actual_size} байт вместо {response_total}"
                        )
                finally:
                    response.close()
                return tmp_file.stat().st_size, transferred

            except (
                    requests.RequestException,
                    CdseQueryError,
            ) as exc:
                if (
                        isinstance(exc, CdseQueryError)
                        and exc.status_code == 416
                        and tmp_file.exists()
                ):
                    rejected_size = tmp_file.stat().st_size
                    tmp_file.unlink(missing_ok=True)
                    _report_progress(progress, -rejected_size)
                    reported_size = 0
                    logger.warning(
                        "CDSE отклонил позицию докачки %s; временный файл "
                        "удалён, следующая попытка начнётся заново",
                        display_name,
                    )
                logger.warning(
                    "Ошибка загрузки %s, попытка %s/%s: %s",
                    display_name,
                    attempt,
                    MAX_RETRIES,
                    exc,
                )

                if attempt == MAX_RETRIES:
                    raise CdseDownloadError(
                        f"Не удалось скачать продукт {display_name}"
                    ) from exc

                delay = 2 ** attempt
                logger.info(
                    "Повтор загрузки %s через %s сек.",
                    display_name,
                    delay,
                )
                time.sleep(delay)

        raise AssertionError("Недостижимое завершение цикла загрузки")

    @staticmethod
    def build_target_dir(
            product: ProductRecord,
            archive_root: str | Path,
    ) -> Path:
        """
        Возвращает каталог архива в формате ``корень/год/тайл``.

        Например: ``/mnt/map/Snapshots/2026/38ULA/``.
        """
        year = product.date[:4] if product.date else "unknown"
        tile = normalize_tile(product.tile) if product.tile else "unknown"
        return ensure_dir(Path(archive_root) / year / tile)

    @staticmethod
    def build_zip_path(target_dir: Path, product: ProductRecord) -> Path:
        """
        Финальный ZIP:
        /mnt/map/Snapshots/2026/38ULA/S2A_MSIL2A_....SAFE.zip
        """
        return target_dir / product.archive_name

    def download_product(
            self,
            product: ProductRecord,
            archive_root: str | Path,
            progress: ProgressCallback | None = None,
    ) -> Path:
        """Скачивание продукта."""
        started = perf_counter()
        target_dir = self.build_target_dir(product, archive_root)
        zip_path = self.build_zip_path(target_dir, product)
        label = f"{product.date} T{normalize_tile(product.tile)}"

        if zip_path.exists():
            if zipfile.is_zipfile(zip_path):
                _report_progress(progress, zip_path.stat().st_size)
                logger.info("Архив уже скачан: %s", zip_path)
                return zip_path
            damaged = zip_path.with_suffix(zip_path.suffix + ".corrupt")
            logger.warning(
                "Повреждённый архив перемещён в %s; запускается повторная загрузка",
                damaged,
            )
            os.replace(zip_path, damaged)

        tmp_file = target_dir / f"{product.archive_name}.tmp"

        # Процесс мог завершиться после полной загрузки, но до rename.
        if tmp_file.exists() and zipfile.is_zipfile(tmp_file):
            _report_progress(progress, tmp_file.stat().st_size)
            os.replace(tmp_file, zip_path)
            logger.info("Готовый временный архив восстановлен: %s", zip_path)
            return zip_path

        logger.info(
            "Начинаем загрузку %s: %s (%s)",
            label,
            product.archive_name,
            _format_size(product.size_bytes),
        )
        final_size, transferred = self._download_with_resume(
            product.product_id,
            tmp_file,
            label=label,
            expected_size=product.size_bytes,
            progress=progress,
        )

        if not zipfile.is_zipfile(tmp_file):
            invalid_size = tmp_file.stat().st_size
            with tmp_file.open("rb") as stream:
                head = stream.read(64)
            tmp_file.unlink(missing_ok=True)
            _report_progress(progress, -invalid_size)
            raise CdseDownloadError(
                f"CDSE вернул не ZIP. HEAD={head}"
            )

        os.replace(tmp_file, zip_path)
        elapsed = max(perf_counter() - started, 0.001)
        logger.info(
            "Загрузка завершена %s: %s за %.1f сек., средняя скорость %s/с",
            label,
            _format_size(final_size),
            elapsed,
            _format_size(round(transferred / elapsed)),
        )

        return zip_path
