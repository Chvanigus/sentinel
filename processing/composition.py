"""Сборка production-зависимостей processing application service."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg2

from core import settings
from core.filesystem import clear_directory_entries_matching
from db.connection import get_database_config
from db.gateway import SqlGateway
from db.repositories import LayerRepository

from .adapters import PostgisFieldDataProvider
from .discovery import ArchivePairFinder
from .layer_metadata import (
    LayerMetadataRefreshService,
    LayerMetadataRefreshSummary,
)
from .service import ProcessingService

if TYPE_CHECKING:
    from .pair_processor import SentinelPairProcessor


class PostgisProcessingStatusReader:
    """PostGIS-адаптер порта статуса обработки."""

    def get_missing_agroids_many(
            self,
            acquired_dates: list[date],
    ) -> dict[date, list[int]]:
        """Одним подключением читает статус опубликованных слоёв набора дат."""
        if not acquired_dates:
            return {}
        with psycopg2.connect(**get_database_config()) as connection:
            repository = LayerRepository(SqlGateway(connection))
            return repository.missing_agroids_many(acquired_dates)


class ProcessingWorkspaceCleaner:
    """Очищает только настроенные рабочие директории processing."""

    def clean(self, acquired_on: date) -> None:
        """Удаляет файлы одной даты, не затрагивая staging неуспешных дат."""
        date_label = acquired_on.strftime("%d_%m_%Y")
        compact_date = acquired_on.strftime("%Y%m%d")
        for directory in (
                settings.INTERMEDIATE,
                settings.PROCESSED_DIR,
                settings.NDVI_DIR,
        ):
            clear_directory_entries_matching(directory, date_label)
        clear_directory_entries_matching(
            settings.TEMP_PROCESSING_DIR,
            compact_date,
        )


def build_pair_processor(
        *,
        recalculate_ndvi: bool = False,
) -> SentinelPairProcessor:
    """Собирает единый обработчик пары из конкретных GIS-зависимостей."""
    from .pair_processor import SentinelPairProcessor
    from .storage import FieldGeometryExporter
    from .workspace import ProcessingOptions, WorkspacePaths

    workspace = WorkspacePaths(
        temporary=Path(settings.TEMP_PROCESSING_DIR),
        intermediate=Path(settings.INTERMEDIATE),
        processed=Path(settings.PROCESSED_DIR),
        ndvi=Path(settings.NDVI_DIR),
    )
    options = ProcessingOptions(
        destination_srid=settings.DESTSRID,
        nodata=settings.NODATA,
    )
    field_data = PostgisFieldDataProvider()
    geometry_exporter = FieldGeometryExporter()
    selected_products = (
        frozenset({"ndvi", "scl"})
        if recalculate_ndvi
        else None
    )

    return SentinelPairProcessor(
        temporary_root=settings.TEMP_PROCESSING_DIR,
        workspace=workspace,
        options=options,
        field_data=field_data,
        geometry_exporter=geometry_exporter,
        products=selected_products,
        ndvi_only=recalculate_ndvi,
        overwrite_statistics=recalculate_ndvi,
    )


def build_processing_service(
        *,
        recalculate_ndvi: bool = False,
) -> ProcessingService:
    """Composition root production-сценария обработки."""
    from satgeo.composition import build_raster_publisher

    return ProcessingService(
        archive_root=settings.ARCHIVE_ROOT,
        pair_finder=ArchivePairFinder(),
        status_reader=PostgisProcessingStatusReader(),
        pair_processor=build_pair_processor(
            recalculate_ndvi=recalculate_ndvi,
        ),
        publisher=build_raster_publisher(
            refresh_products={"ndvi"} if recalculate_ndvi else (),
        ),
        cleaner=ProcessingWorkspaceCleaner(),
        process_completed=recalculate_ndvi,
        clean_before_each=recalculate_ndvi,
    )


def refresh_layer_metadata(
        *,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
) -> LayerMetadataRefreshSummary:
    """Обновляет метаданные слоёв в одном подключении без запуска GDAL."""
    with psycopg2.connect(**get_database_config()) as connection:
        service = LayerMetadataRefreshService(
            archive_root=settings.ARCHIVE_ROOT,
            pair_finder=ArchivePairFinder(),
            writer=LayerRepository(SqlGateway(connection)),
        )
        return service.run(
            start_date=start_date,
            end_date=end_date,
        )
