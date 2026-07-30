"""Модели CDSE."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class ProductRecord:
    """Нормализованная запись продукта."""
    product_id: str
    name: str
    tile: str
    date: str
    cloud_cover: Optional[float]
    size_bytes: Optional[int]
    exists: bool = False
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @property
    def archive_stem(self) -> str:
        """Убирает расширение .safe из имени продукта."""
        name = self.name.strip()
        if name.upper().endswith(".SAFE"):
            name = name[:-5]
        return name

    @property
    def archive_name(self) -> str:
        """Возвращает имя архива для скачивания с сервера."""
        return f"{self.archive_stem}.zip"