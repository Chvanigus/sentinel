"""Быстрое обновление метаданных уже опубликованных спутниковых слоёв."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Protocol

from core.logging import get_logger
from domain.models import LayerMetadataUpdate

from .discovery import ArchivePairFinder
from .domain import build_layer_metadata_updates


class LayerMetadataWriter(Protocol):
    """Порт пакетного обновления метаданных опубликованных слоёв."""

    def refresh_metadata(
            self,
            updates: list[LayerMetadataUpdate],
    ) -> int:
        """Обновляет существующие слои и возвращает количество строк."""
        ...


@dataclass(frozen=True)
class LayerMetadataRefreshSummary:
    """Итог обновления метаданных без обработки растров."""

    discovered_pairs: int
    selected_pairs: int
    prepared_updates: int
    updated_layers: int


class LayerMetadataRefreshService:
    """Собирает метаданные из ZIP-пар и пакетно обновляет ``maps_layer``."""

    def __init__(
            self,
            archive_root: str | Path,
            pair_finder: ArchivePairFinder,
            writer: LayerMetadataWriter,
    ) -> None:
        self.archive_root = Path(archive_root)
        self.pair_finder = pair_finder
        self.writer = writer
        self.logger = get_logger(self.__class__.__name__)

    def run(
            self,
            *,
            start_date: datetime | None = None,
            end_date: datetime | None = None,
    ) -> LayerMetadataRefreshSummary:
        """Обновляет метаданные найденных пар внутри выбранного периода."""
        started = perf_counter()
        pairs = self.pair_finder.find(
            self.archive_root,
            start_date=start_date,
            end_date=end_date,
        )
        selected = [
            pair
            for pair in pairs
            if self._inside_period(pair.acquired_at, start_date, end_date)
        ]
        updates_by_key = {
            (update.acquired_on, update.agroid): update
            for pair in selected
            for update in build_layer_metadata_updates(pair)
        }
        updates = list(updates_by_key.values())
        updated_layers = self.writer.refresh_metadata(updates)
        summary = LayerMetadataRefreshSummary(
            discovered_pairs=len(pairs),
            selected_pairs=len(selected),
            prepared_updates=len(updates),
            updated_layers=updated_layers,
        )
        self.logger.info(
            "METADATA OK: найдено пар=%d выбрано=%d "
            "хозяйств=%d обновлено слоёв=%d | %.2f сек.",
            summary.discovered_pairs,
            summary.selected_pairs,
            summary.prepared_updates,
            summary.updated_layers,
            perf_counter() - started,
        )
        return summary

    @staticmethod
    def _inside_period(
            acquired_at: datetime,
            start_date: datetime | None,
            end_date: datetime | None,
    ) -> bool:
        """Проверяет попадание времени съёмки в полуоткрытый диапазон дат."""
        acquired_on = acquired_at.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
            tzinfo=None,
        )
        return not (
            start_date is not None and acquired_on < start_date
            or end_date is not None and acquired_on >= end_date
        )
