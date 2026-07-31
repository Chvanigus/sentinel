"""Модуль скачивания данных из CDSE."""
from __future__ import annotations

import os
import time
import zipfile
from pathlib import Path

import requests

from core.logging import get_logger

from .client import CdseODataClient
from .exceptions import CdseDownloadError, CdseQueryError
from .models import ProductRecord
from .utils import ensure_dir, normalize_tile

logger = get_logger("CdseDownloader")

MAX_RETRIES = 5
CHUNK_SIZE = 4 * 1024 * 1024


class ODataProductDownloader:
    """
    Скачивание продукта CDSE и упаковка SAFE в ZIP для долгосрочного хранения.
    """

    def __init__(self, client: CdseODataClient):
        self.client = client

    def _download_with_resume(self, product_id: str, tmp_file: Path):
        """
        Надёжная загрузка с retry и докачкой
        """

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                downloaded = tmp_file.stat().st_size if tmp_file.exists() else 0

                headers = {}
                if downloaded > 0:
                    headers["Range"] = f"bytes={downloaded}-"
                    logger.warning(
                        "Resume download from %s bytes", downloaded
                    )

                response = self.client.download_stream(
                    product_id, authorized=True, headers=headers
                )
                try:
                    response.raise_for_status()

                    # Некоторые серверы игнорируют Range и отвечают 200 полным
                    # файлом. В таком случае нельзя дописывать его к partial ZIP.
                    if downloaded > 0 and response.status_code != 206:
                        logger.warning(
                            "Сервер не подтвердил Range; загрузка начата заново"
                        )
                        downloaded = 0

                    mode = "ab" if downloaded > 0 else "wb"
                    with open(tmp_file, mode) as file_obj:
                        for chunk in response.iter_content(CHUNK_SIZE):
                            if chunk:
                                file_obj.write(chunk)
                finally:
                    response.close()
                return

            except (
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    CdseQueryError,
            ) as exc:
                logger.warning(
                    "Download failed (attempt %s/%s): %s",
                    attempt,
                    MAX_RETRIES,
                    exc,
                )

                if attempt == MAX_RETRIES:
                    raise CdseDownloadError(
                        f"Не удалось скачать продукт {product_id}"
                    ) from exc

                time.sleep(2 ** attempt)

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
    ) -> Path:
        """Скачивание продукта."""
        target_dir = self.build_target_dir(product, archive_root)
        ensure_dir(target_dir)

        zip_path = self.build_zip_path(target_dir, product)

        if zip_path.exists():
            if zipfile.is_zipfile(zip_path):
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
            os.replace(tmp_file, zip_path)
            return zip_path

        self._download_with_resume(product.product_id, tmp_file)

        if not zipfile.is_zipfile(tmp_file):
            head = tmp_file.read_bytes()[:64]
            tmp_file.unlink(missing_ok=True)
            raise CdseDownloadError(
                f"CDSE вернул не ZIP. HEAD={head}"
            )

        os.replace(tmp_file, zip_path)

        return zip_path
