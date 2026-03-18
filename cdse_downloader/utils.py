"""Утилиты модуля cdse_downloader."""
import os


def build_archive_index(base_path="/mnt/map/Snapshots") -> set[str]:
    """
    Возвращает set имён архивов без .zip
    """
    existing = set()

    if not os.path.exists(base_path):
        return existing

    for root, _, files in os.walk(base_path):
        for f in files:
            if f.endswith(".zip"):
                existing.add(f[:-4])

    return existing