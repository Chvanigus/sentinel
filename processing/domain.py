"""Доменная модель сценария обработки спутниковых снимков."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any


class ProductLevel(StrEnum):
    """Поддерживаемые уровни продуктов Sentinel-2."""

    L1C = "msil1c"
    L2A = "msil2a"

    @classmethod
    def parse(cls, value: str) -> ProductLevel:
        """Нормализует строковое обозначение уровня продукта."""
        normalized = value.strip().lower()
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError(
                f"Неподдерживаемый уровень продукта: {value}"
            ) from exc

    @property
    def required_bands(self) -> tuple[str, ...]:
        """Возвращает минимальный набор файлов для обработки продукта."""
        common = ("TCI", "B03", "B04", "B08")
        return common if self is ProductLevel.L1C else (*common, "SCL")


AGROIDS_BY_TILE: dict[str, tuple[int, ...]] = {
    "t38ula": (1, 3, 4),
    "t38ulb": (1, 5, 6),
}


@dataclass(frozen=True)
class BandOffsets:
    """Аддитивные radiometric offset нужных спектральных каналов."""

    b03: float = 0.0
    b04: float = 0.0
    b08: float = 0.0


@dataclass(frozen=True)
class SceneContext:
    """Нормализованный контекст обработки одного спутникового продукта."""

    archive_path: Path
    tile: str
    acquired_on: date
    satellite: str
    level: ProductLevel
    agroids: tuple[int, ...]
    band_offsets: BandOffsets = field(default_factory=BandOffsets)

    @classmethod
    def from_zip_info(
            cls,
            archive_path: str | Path,
            zip_info: Any,
            band_offsets: BandOffsets | None = None,
    ) -> SceneContext:
        """Создаёт контекст сцены из метаданных распаковываемого архива."""
        tile = zip_info.tile.strip().lower()
        try:
            agroids = AGROIDS_BY_TILE[tile]
        except KeyError as exc:
            raise ValueError(
                f"Для тайла {zip_info.tile} не настроены агропредприятия"
            ) from exc

        return cls(
            archive_path=Path(archive_path),
            tile=tile,
            acquired_on=zip_info.date,
            satellite=zip_info.satellite.strip().lower(),
            level=ProductLevel.parse(zip_info.level),
            agroids=agroids,
            band_offsets=band_offsets or BandOffsets(),
        )

    @property
    def date_label(self) -> str:
        """Дата в историческом формате имён файлов проекта."""
        return self.acquired_on.strftime("%d_%m_%Y")

@dataclass(frozen=True)
class ArchivePair:
    """Пара соседних ULA/ULB архивов за одну дату."""

    acquired_at: datetime
    prefix: str
    ula: Path
    ulb: Path

    @property
    def acquired_on(self) -> date:
        """Возвращает календарную дату получения снимка."""
        return self.acquired_at.date()

    @property
    def archives(self) -> tuple[Path, Path]:
        """Возвращает архивы пары в стабильном порядке ULA, ULB."""
        return self.ula, self.ulb


@dataclass(frozen=True)
class ProcessingRunSummary:
    """Итог выполнения одного запуска обработки."""

    discovered: int
    selected: int
    processed: int
    skipped: int
