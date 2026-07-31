"""Утилиты модуля cdse_downloader."""
from __future__ import annotations

import os
import re
import shutil
from datetime import date, timedelta
from pathlib import Path

from core.logging import get_logger

logger = get_logger("CdseUtils")

_TILE_RE = re.compile(r"_T(\d{2}[A-Z]{3})_")


def build_archive_index(base_path: str | Path) -> set[str]:
    """Возвращает список существующих архивов в base_path."""
    existing: set[str] = set()

    if not os.path.exists(base_path):
        return existing

    for _root, _, files in os.walk(base_path):
        for f in files:
            if f.endswith(".zip"):
                existing.add(f)

    return existing


def ensure_dir(path: str | Path) -> Path:
    """Создаёт директорию и возвращает Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def human_size(size_bytes: int | None) -> str:
    """Человекочитаемый размер."""
    if not size_bytes:
        return "-"
    size = float(size_bytes)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} ПБ"


def disk_usage(path: str) -> tuple[int, int]:
    """Возвращает free/total."""
    usage = shutil.disk_usage(path)
    return usage.free, usage.total


def normalize_cdse_datetime(value: str, is_start: bool) -> str:
    """
    Приводит дату/время к ISO-формату CDSE.
    """
    value = value.strip()

    if any(sep in value for sep in ("T", "t", "_", " ")):
        return value if value.endswith("Z") else value

    if is_start:
        return f"{value}T00:00:00.000Z"
    return f"{value}T23:59:59.999Z"


def normalize_cdse_interval(start: str, end: str) -> str:
    """interval для ContentDate."""
    return f"{normalize_cdse_datetime(start, True)}/{normalize_cdse_datetime(end, False)}"


def normalize_tile(tile: str | None) -> str:
    """
    Нормализует тайл к виду 38ULA.
    """
    tile = (tile or "").upper().strip()
    if not tile:
        return ""
    if tile.startswith("T"):
        return tile[1:]
    return tile


def extract_tile_from_name(name: str | None) -> str:
    """
    Достаёт тайл из имени продукта.
    Возвращает 38ULA (без T).
    """
    if not name:
        return ""
    match = _TILE_RE.search(name.upper())
    if not match:
        return ""
    return match.group(1)


def extract_tile_from_item(item: dict) -> str:
    """
    Сначала пробует tileId из Attributes, потом имя продукта.
    """
    attrs = item.get("Attributes") or []
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        if attr.get("Name") == "tileId":
            return normalize_tile(str(attr.get("Value") or ""))
    return extract_tile_from_name(str(item.get("Name") or ""))


def split_date_range(
        start: str, end: str, chunk_days: int = 1
) -> list[tuple[str, str]]:
    """
    Делит диапазон на куски по N дней.
    """
    if chunk_days < 1:
        raise ValueError("chunk_days должен быть положительным числом")

    start_d = date.fromisoformat(start[:10])
    end_d = date.fromisoformat(end[:10])
    if start_d > end_d:
        raise ValueError("Дата начала не может быть позже даты окончания")

    ranges: list[tuple[str, str]] = []
    cur = start_d

    while cur <= end_d:
        nxt = min(cur + timedelta(days=chunk_days), end_d + timedelta(days=1))
        ranges.append(
            (
                f"{cur.isoformat()}T00:00:00.000Z",
                f"{nxt.isoformat()}T00:00:00.000Z",
            )
        )
        cur = nxt

    return ranges


def normalize_product_name(name: str) -> str:
    """
    Приводит:
    S2C_MSIL2A_....SAFE
    S2C_MSIL2A_....zip

    к одному ключу (SAFE root id)
    """
    name = name.strip()

    if name.endswith(".zip"):
        name = name[:-4]

    if name.endswith(".SAFE"):
        name = name[:-5]

    return name
