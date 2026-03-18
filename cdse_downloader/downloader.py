"""S3 клиент."""
import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import boto3
import botocore
from tqdm import tqdm

from core.logging import get_logger

logger = get_logger("S3Downloader")


class S3Downloader:
    """S3 клиент для рекурсивного скачивания SAFE-папок Sentinel-2."""

    def __init__(self, aws_access_key_id: str,
                 aws_secret_access_key: str,
                 endpoint_url: str):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url
        )

    def download_folder(self,
                        s3_url: str,
                        local_dir: str,
                        max_workers: int = 1,
                        progress_position: int = None,
                        progress_desc: str  = None) -> None:
        """
        Рекурсивно скачивает все файлы из s3://bucket/prefix/ в local_dir.
        Если хотя бы один файл не скачался — возбуждает исключение RuntimeError.
        """
        parsed = urlparse(s3_url)
        if not parsed.scheme == "s3":
            raise ValueError(f"Некорректный S3 URL: {s3_url}")
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        os.makedirs(local_dir, exist_ok=True)

        paginator = self.s3.get_paginator("list_objects_v2")
        tasks = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                rel_path = os.path.relpath(key, prefix)
                local_path = os.path.join(local_dir, rel_path)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                tasks.append((bucket, key, local_path))

        if not tasks:
            logger.warning(
                "Пустая папка S3: %s",
                s3_url
            )
            return

        failed = []
        total_files = len(tasks)

        # создаём tqdm для этого архива
        pbar = tqdm(
            total=total_files,
            desc=(progress_desc or "Скачивание снимка"),
            unit="файл",
            position=progress_position if progress_position is not None else 0,
            leave=True,
            dynamic_ncols=True,
        )

        def _dl(task):
            b, k, lp = task
            try:
                # скачиваем
                self.s3.download_file(b, k, lp)
                # после успешной загрузки увеличиваем progress
                pbar.update(1)
            except botocore.exceptions.ClientError as exc:
                logger.exception("Ошибка скачивания %s: %s", k, exc)
                failed.append(k)
                pbar.update(1)
                raise

        max_workers = max(1, int(max_workers))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_dl, t) for t in tasks]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass

        pbar.close()

        if failed:
            sample = failed[:10]
            raise RuntimeError(
                f"Не удалось скачать {len(failed)} файлов из {len(tasks)}. "
                f"Примеры: {sample}"
            )

    @staticmethod
    def make_zip(local_dir: str, zip_path: str) -> None:
        """Создает ZIP из локальной папки (включая структуру SAFE)."""
        if os.path.exists(zip_path):
            return
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(local_dir):
                for f in files:
                    abs_path = os.path.join(root, f)
                    rel_path = os.path.relpath(abs_path, local_dir)
                    zf.write(abs_path, rel_path)
        shutil.rmtree(local_dir, ignore_errors=True)
