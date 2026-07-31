"""Модуль отчёта по найденным продуктам."""
from __future__ import annotations

import os

from core.logging import get_logger

from .models import ProductRecord
from .utils import disk_usage, human_size

logger = get_logger("CdseReport")


def print_products_report(
        records: list[ProductRecord],
        archive_base: str,
) -> None:
    """
    Печатает таблицу и сводку.
    """
    print()
    print(
        f"{'№ п/п':<5} | "
        f"{'Квадрат':<8} | "
        f"{'Дата':<10} | "
        f"{'Облачность %':<12} | "
        f"{'Размер':<10} | "
        f"{'Есть в архиве':<14} | "
        f"Полное название"
    )
    print("-" * 137)

    for i, r in enumerate(records, start=1):
        cloud_str = f"{r.cloud_cover:.2f}" if isinstance(r.cloud_cover,
                                                         (int, float)) else "-"
        size_str = human_size(r.size_bytes)
        exists_str = "✅" if r.exists else "❌"
        print(
            f"{i:>5} | "
            f"{r.tile:<8} | "
            f"{r.date:<10} | "
            f"{cloud_str:>12} | "
            f"{size_str:>10} | "
            f"{exists_str:<13} | "
            f"{r.name}"
        )

    print()
    total_size_bytes = sum(r.size_bytes for r in records if
                           isinstance(r.size_bytes, (int, float)))
    download_size_bytes = sum(
        r.size_bytes for r in records
        if not r.exists and isinstance(r.size_bytes, (int, float))
    )
    total_count = len(records)
    exists_count = sum(1 for r in records if r.exists)
    download_count = total_count - exists_count

    if download_count == 0:
        logger.info("🎉 Все снимки уже скачаны, загрузка не требуется")

    try:
        free_bytes, total_bytes = disk_usage(archive_base)
        checked_path = archive_base
    except Exception as exc:
        logger.warning(
            "Не удалось получить информацию о диске для %s: %s. Будем использовать cwd.",
            archive_base, exc
        )
        free_bytes, total_bytes = disk_usage(os.getcwd())
        checked_path = os.getcwd()

    print(f"Информация проверена по пути: {checked_path}")
    print("=" * 137)
    print("Сводка по загрузке:")
    print(f"Всего найдено снимков        : {total_count}")
    print(f"Уже в архиве                 : {exists_count}")
    print(f"Будет скачано                : {download_count}")
    print("-" * 137)
    print(f"Общий размер снимков (всех)  : {human_size(total_size_bytes)}")
    print(f"Размер к скачиванию          : {human_size(download_size_bytes)}")
    print(f"Свободно на диске            : {human_size(free_bytes)}")
    print(f"Всего на диске               : {human_size(total_bytes)}")

    if download_size_bytes > free_bytes:
        deficit = download_size_bytes - free_bytes
        print("❌ Недостаточно места для скачивания на архиве!")
        print(f"Не хватает: {human_size(deficit)}")
    else:
        remaining = free_bytes - download_size_bytes
        print("✅ Места достаточно для скачивания на архиве")
        print(f"Останется после загрузки: {human_size(remaining)}")

    print("=" * 137)
    print()
