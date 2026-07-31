"""Application service полного сценария обработки архива."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Protocol

from core.logging import get_logger

from .discovery import ArchivePairFinder
from .domain import ProcessingRunSummary
from .exceptions import ProcessingRunError


class ProcessingStatusReader(Protocol):
    """Порт чтения статуса обработки из внешнего хранилища."""

    def get_missing_agroids(self, acquired_on: date) -> list[int]:
        """Возвращает хозяйства, для которых ещё нужны результаты."""
        ...


class SceneArchiveProcessor(Protocol):
    """Порт обработки одного ZIP-архива."""

    def process(self, archive_path: Path) -> None:
        """Обрабатывает один архив сцены."""
        ...


class ResultPublisher(Protocol):
    """Порт публикации результатов обработанной пары."""

    def publish_date(self, acquired_on: date) -> None:
        """Публикует готовые результаты указанной даты."""
        ...


class WorkspaceCleaner(Protocol):
    """Порт очистки временного рабочего пространства."""

    def clean(self) -> None:
        """Удаляет промежуточные файлы текущей попытки."""
        ...


class ProcessingService:
    """Координирует use case, не импортируя PostGIS, GDAL и GeoServer."""

    def __init__(
            self,
            archive_root: str | Path,
            pair_finder: ArchivePairFinder,
            status_reader: ProcessingStatusReader,
            scene_processor: SceneArchiveProcessor,
            publisher: ResultPublisher,
            cleaner: WorkspaceCleaner,
    ):
        self.archive_root = Path(archive_root)
        self.pair_finder = pair_finder
        self.status_reader = status_reader
        self.scene_processor = scene_processor
        self.publisher = publisher
        self.cleaner = cleaner
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
        selected = []
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

            if not debug:
                missing = self.status_reader.get_missing_agroids(
                    pair.acquired_on
                )
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
                for archive in pair.archives:
                    archive_started = perf_counter()
                    self.logger.info(
                        "ARCHIVE START: %s | %s",
                        date_label,
                        archive.name,
                    )
                    self.scene_processor.process(archive)
                    self.logger.info(
                        "ARCHIVE OK: %s | %s | %.2f сек.",
                        date_label,
                        archive.name,
                        perf_counter() - archive_started,
                    )
                publish_started = perf_counter()
                self.publisher.publish_date(pair.acquired_on)
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
            finally:
                if not debug:
                    cleanup_started = perf_counter()
                    try:
                        self.cleaner.clean()
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
