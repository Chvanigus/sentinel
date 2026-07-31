"""Сборка production-зависимостей processing application service."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from time import perf_counter

import psycopg2

from core import settings
from core.filesystem import clear_directory_contents
from core.logging import get_logger
from db.connection import get_database_config
from db.gateway import SqlGateway
from db.repositories import LayerRepository

from .adapters import PostgisFieldDataProvider
from .archive import SentinelArchive
from .discovery import ArchivePairFinder
from .domain import ProductLevel, SceneContext
from .paths import (
    CloudMaskPaths,
    L1CProductPaths,
    L2AProductPaths,
    MosaicPaths,
    NdviStatisticsPaths,
    SentinelCropPaths,
)
from .pipeline import ScenePipeline, SceneStep
from .service import ProcessingService
from .storage import FieldGeometryExporter, GeowareTileArchive
from .workspace import ProcessingOptions, WorkspacePaths


class PostgisProcessingStatusReader:
    """PostGIS-адаптер порта статуса обработки."""

    def get_missing_agroids(self, acquired_on: date) -> list[int]:
        """Возвращает хозяйства с неполным набором опубликованных слоёв."""
        with psycopg2.connect(**get_database_config()) as connection:
            repository = LayerRepository(SqlGateway(connection))
            return repository.missing_agroids(acquired_on)


class DefaultSceneArchiveProcessor:
    """Распаковывает архив и передаёт сцену в processing pipeline."""

    def __init__(
            self,
            pipeline: ScenePipeline,
            temporary_root: str | Path,
    ):
        self.pipeline = pipeline
        self.temporary_root = Path(temporary_root)
        self.logger = get_logger(self.__class__.__name__)

    def process(self, archive_path: Path) -> None:
        """Распаковывает обязательные каналы и выполняет pipeline сцены."""
        archive = SentinelArchive(archive_path)
        scene = SceneContext.from_zip_info(
            archive_path,
            archive.metadata,
        )

        extraction_started = perf_counter()
        extracted_path = archive.extract(
            self.temporary_root,
            scene.level.required_bands,
        )
        self.logger.info(
            "EXTRACT OK: %s → %s | %.2f сек.",
            archive_path.name,
            extracted_path,
            perf_counter() - extraction_started,
        )

        self.pipeline.run(scene)


class ProcessingWorkspaceCleaner:
    """Очищает только настроенные рабочие директории processing."""

    def clean(self) -> None:
        """Удаляет промежуточные результаты завершённой попытки."""
        clear_directory_contents(
            settings.INTERMEDIATE,
            settings.PROCESSED_DIR,
            settings.NDVI_DIR,
            settings.TEMP_PROCESSING_DIR,
        )


def build_scene_pipeline() -> ScenePipeline:
    """Создаёт pipeline, локализуя импорты тяжёлого GDAL-слоя."""
    from processing.processors.cloudmask import (
        FilterNDVIProcessor,
        RescaleSCLProcessor,
    )
    from processing.processors.combine import MosaicProcessor
    from processing.processors.ndvistat import (
        NdviStatisticsProcessor,
    )
    from processing.processors.sentinel import AgroCropProcessor
    from processing.processors.tiles import TileImageProcessor

    workspace = WorkspacePaths(
        temporary=Path(settings.TEMP_PROCESSING_DIR),
        intermediate=Path(settings.INTERMEDIATE),
        processed=Path(settings.PROCESSED_DIR),
        ndvi=Path(settings.NDVI_DIR),
        geoware=Path(settings.GS_DATA_ROOT),
    )
    options = ProcessingOptions(
        destination_srid=settings.DESTSRID,
        nodata=settings.NODATA,
    )
    field_data = PostgisFieldDataProvider()
    output_archive = GeowareTileArchive(workspace.geoware)
    geometry_exporter = FieldGeometryExporter()

    def process_tile(scene: SceneContext) -> None:
        """Строит tile-level растры и сохраняет долговременные результаты."""
        path_type = (
            L1CProductPaths
            if scene.level is ProductLevel.L1C
            else L2AProductPaths
        )
        paths = path_type(scene, workspace)
        TileImageProcessor(
            scene,
            paths,
            output_archive,
            options,
        ).run()

    def process_agroids(scene: SceneContext) -> None:
        """Вырезает tile-level продукты по границам хозяйств."""
        paths = SentinelCropPaths(scene, workspace)
        AgroCropProcessor(
            scene,
            paths,
            field_data,
            options,
        ).run()

    def combine_tiles(scene: SceneContext) -> None:
        """Объединяет фрагменты соседних тайлов для общего хозяйства."""
        MosaicProcessor(
            scene,
            MosaicPaths(scene, workspace),
        ).run()

    def apply_cloud_mask(scene: SceneContext) -> None:
        """Фильтрует NDVI L2A по классификации сцены."""
        if scene.level is ProductLevel.L1C:
            return
        paths = CloudMaskPaths(scene, workspace)
        RescaleSCLProcessor(scene, paths).run()
        FilterNDVIProcessor(scene, paths, options).run()

    def collect_statistics(scene: SceneContext) -> None:
        """Рассчитывает и сохраняет полевую статистику NDVI."""
        NdviStatisticsProcessor(
            scene,
            NdviStatisticsPaths(scene, workspace),
            field_data,
            geometry_exporter,
            options.nodata,
        ).run()

    return ScenePipeline(
        [
            SceneStep("tile", process_tile),
            SceneStep("sentinel", process_agroids),
            SceneStep("combine", combine_tiles),
            SceneStep("cloud-mask", apply_cloud_mask),
            SceneStep("ndvi-statistics", collect_statistics),
        ]
    )


def build_processing_service() -> ProcessingService:
    """Composition root production-сценария обработки."""
    from satgeo.composition import build_raster_publisher

    return ProcessingService(
        archive_root=settings.ARCHIVE_ROOT,
        pair_finder=ArchivePairFinder(),
        status_reader=PostgisProcessingStatusReader(),
        scene_processor=DefaultSceneArchiveProcessor(
            build_scene_pipeline(),
            settings.TEMP_PROCESSING_DIR,
        ),
        publisher=build_raster_publisher(),
        cleaner=ProcessingWorkspaceCleaner(),
    )
