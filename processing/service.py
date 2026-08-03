"""Application service полного сценария обработки архива."""
from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from time import perf_counter
from typing import Protocol

from core.logging import get_logger
from domain.models import LayerSourceMetadata

from .discovery import ArchivePairFinder
from .domain import (
    AGROIDS_BY_TILE,
    PROCESSING_ALGORITHM_VERSION,
    ProcessingRunSummary,
)
from .exceptions import ProcessingRunError


class ProcessingStatusReader(Protocol):
    """Порт чтения статуса обработки из внешнего хранилища."""

    def get_missing_agroids_many(
            self,
            acquired_dates: list[date],
    ) -> dict[date, list[int]]:
        """Возвращает недостающие хозяйства сразу для набора дат."""
        ...


class ArchivePairProcessor(Protocol):
    """Порт обработки согласованной пары ZIP-архивов."""

    def process(self, pair) -> None:
        """Обрабатывает оба архива и общие этапы одной даты."""
        ...


class ResultPublisher(Protocol):
    """Порт публикации результатов обработанной пары."""

    def publish_date(
            self,
            acquired_on: date,
            source: LayerSourceMetadata,
    ) -> None:
        """Публикует готовые результаты даты вместе с метаданными источника."""
        ...


class WorkspaceCleaner(Protocol):
    """Порт очистки временного рабочего пространства."""

    def clean(self, acquired_on: date) -> None:
        """Удаляет только промежуточные файлы указанной даты."""
        ...


class ProcessingService:
    """Координирует use case, не импортируя PostGIS, GDAL и GeoServer."""

    def __init__(
            self,
            archive_root: str | Path,
            pair_finder: ArchivePairFinder,
            status_reader: ProcessingStatusReader,
            pair_processor: ArchivePairProcessor,
            publisher: ResultPublisher,
            cleaner: WorkspaceCleaner,
            process_completed: bool = False,
            clean_before_each: bool = False,
    ):
        self.archive_root = Path(archive_root)
        self.pair_finder = pair_finder
        self.status_reader = status_reader
        self.pair_processor = pair_processor
        self.publisher = publisher
        self.cleaner = cleaner
        self.process_completed = process_completed
        self.clean_before_each = clean_before_each
        self.logger = get_logger(self.__class__.__name__)

    def run(
            self,
            archive_root: str | Path | None = None,
            debug: bool = False,
            start_date: datetime | None = None,
            end_date: datetime | None = None,
    ) -> ProcessingRunSummary:
        """Отбирает пары по периоду, обрабатывает и публикует каждую дату."""
        run_started = perf_counter()
        root = Path(archive_root) if archive_root else self.archive_root
        pairs = self.pair_finder.find(root)
        candidates = []
        skipped = 0

        self.logger.info("Сканируем архив: %s", root)
        self.logger.info("Найдено валидных пар: %d", len(pairs))

        for pair in pairs:
            acquired_at = datetime.combine(
                pair.acquired_on, datetime.min.time()
            )
            if start_date and acquired_at < start_date:
                continue
            if end_date and acquired_at >= end_date:
                continue

            candidates.append(pair)

        missing_by_date = {}
        if not debug and not self.process_completed:
            missing_by_date = self.status_reader.get_missing_agroids_many(
                list(dict.fromkeys(pair.acquired_on for pair in candidates))
            )

        selected = []
        for pair in candidates:
            if not debug and not self.process_completed:
                missing = missing_by_date[pair.acquired_on]
                if not missing:
                    skipped += 1
                    self.logger.info(
                        "SKIP %s → все результаты уже существуют",
                        pair.acquired_on,
                    )
                    continue
                self.logger.info(
                    "PROCESS %s → нет агро: %s",
                    pair.acquired_on,
                    ", ".join(map(str, missing)),
                )
            elif self.process_completed:
                self.logger.info(
                    "RECALCULATE %s → принудительная обработка NDVI",
                    pair.acquired_on,
                )

            selected.append(pair)

        failed_dates: list[str] = []
        processed = 0
        for index, pair in enumerate(selected, start=1):
            pair_started = perf_counter()
            date_label = pair.acquired_on.isoformat()
            self.logger.info(
                "[%d/%d] Обработка даты: %s",
                index,
                len(selected),
                date_label,
            )
            try:
                if self.clean_before_each:
                    cleanup_started = perf_counter()
                    self.cleaner.clean(pair.acquired_on)
                    self.logger.info(
                        "PRE-CLEANUP OK: %s | %.2f сек.",
                        date_label,
                        perf_counter() - cleanup_started,
                    )
                processing_started = perf_counter()
                self.pair_processor.process(pair)
                self.logger.info(
                    "PROCESSING OK: %s | %.2f сек.",
                    date_label,
                    perf_counter() - processing_started,
                )
                publish_started = perf_counter()
                self.publisher.publish_date(
                    pair.acquired_on,
                    self._source_metadata(pair),
                )
                self.logger.info(
                    "PUBLISH OK: %s | %.2f сек.",
                    date_label,
                    perf_counter() - publish_started,
                )
                processed += 1
                self.logger.info(
                    "SUCCESS %s | %.2f сек.",
                    date_label,
                    perf_counter() - pair_started,
                )
            except Exception as exc:
                failed_dates.append(date_label)
                self.logger.exception(
                    "FAIL %s | %.2f сек.: %s",
                    date_label,
                    perf_counter() - pair_started,
                    exc,
                )
            else:
                if not debug:
                    cleanup_started = perf_counter()
                    try:
                        self.cleaner.clean(pair.acquired_on)
                        self.logger.info(
                            "CLEANUP OK: %s | %.2f сек.",
                            date_label,
                            perf_counter() - cleanup_started,
                        )
                    except Exception as exc:
                        self.logger.exception(
                            "Ошибка очистки после %s: %s",
                            date_label,
                            exc,
                        )
                        if date_label not in failed_dates:
                            failed_dates.append(date_label)

        if failed_dates:
            self.logger.error(
                "RUN FAIL: ошибок=%d даты=%s | %.2f сек.",
                len(failed_dates),
                ", ".join(failed_dates),
                perf_counter() - run_started,
            )
            raise ProcessingRunError(failed_dates)

        summary = ProcessingRunSummary(
            discovered=len(pairs),
            selected=len(selected),
            processed=processed,
            skipped=skipped,
        )
        self.logger.info(
            "RUN OK: найдено=%d выбрано=%d обработано=%d "
            "пропущено=%d | %.2f сек.",
            summary.discovered,
            summary.selected,
            summary.processed,
            summary.skipped,
            perf_counter() - run_started,
        )
        return summary

    @staticmethod
    def _source_metadata(pair) -> LayerSourceMetadata:
        """Строит метаданные публикации и привязку хозяйств к тайлам пары."""
        by_agroid: dict[int, list[str]] = {}
        for tile, agroids in AGROIDS_BY_TILE.items():
            source_tile = tile.upper()
            for agroid in agroids:
                by_agroid.setdefault(agroid, []).append(source_tile)
        acquired_at = pair.acquired_at
        if acquired_at.tzinfo is None:
            acquired_at = acquired_at.replace(tzinfo=UTC)
        return LayerSourceMetadata(
            acquired_at=acquired_at,
            satellite=pair.satellite.upper(),
            source_level=pair.level.name,
            processing_baseline=pair.processing_baseline,
            source_tiles_by_agroid={
                agroid: tuple(tiles)
                for agroid, tiles in by_agroid.items()
            },
            algorithm_version=PROCESSING_ALGORITHM_VERSION,
        )
