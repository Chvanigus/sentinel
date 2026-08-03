"""Обработка согласованной пары соседних архивов Sentinel."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from time import perf_counter

from core.logging import get_logger

from .archive import SentinelArchive
from .domain import ArchivePair, ProductLevel, SceneContext
from .exceptions import ProcessingStepError
from .paths import (
    CloudMaskPaths,
    L1CProductPaths,
    L2AProductPaths,
    MosaicPaths,
    NdviStatisticsPaths,
    SentinelCropPaths,
)
from .processors.cloudmask import FilterNDVIProcessor, RescaleSCLProcessor
from .processors.combine import MosaicProcessor
from .processors.ndvistat import NdviStatisticsProcessor
from .processors.sentinel import AgroCropProcessor
from .processors.tiles import TileImageProcessor


class SentinelPairProcessor:
    """Готовит оба тайла и один раз завершает общие этапы даты."""

    def __init__(
            self,
            *,
            temporary_root: str | Path,
            workspace,
            options,
            field_data,
            output_archive,
            geometry_exporter,
            products=None,
            ndvi_only: bool = False,
            overwrite_statistics: bool = False,
    ) -> None:
        self.temporary_root = Path(temporary_root)
        self.workspace = workspace
        self.options = options
        self.field_data = field_data
        self.output_archive = output_archive
        self.geometry_exporter = geometry_exporter
        self.products = products
        self.ndvi_only = ndvi_only
        self.overwrite_statistics = overwrite_statistics
        self.logger = get_logger(self.__class__.__name__)

    def process(self, pair: ArchivePair) -> None:
        """Обрабатывает два тайла пары и выполняет общие этапы ровно один раз."""
        pair_started = perf_counter()
        scenes = []
        for archive_path in pair.archives:
            scene = self._run_step(
                "extract",
                archive_path.name,
                lambda path=archive_path: self._extract_scene(path),
            )
            self._run_step(
                "tile",
                self._scene_label(scene),
                lambda current=scene: self._process_tile(current),
            )
            self._run_step(
                "crop",
                self._scene_label(scene),
                lambda current=scene: self._crop_agroids(current),
            )
            scenes.append(scene)

        final_scene = self._build_final_scene(pair, scenes)
        self._run_step(
            "combine",
            self._scene_label(final_scene),
            lambda: self._combine(final_scene),
        )
        self._run_step(
            "cloud-mask",
            self._scene_label(final_scene),
            lambda: self._apply_cloud_mask(final_scene),
        )
        self._run_step(
            "ndvi-statistics",
            self._scene_label(final_scene),
            lambda: self._collect_statistics(final_scene),
        )
        self.logger.info(
            "PAIR PIPELINE OK: %s | %.2f сек.",
            pair.acquired_on,
            perf_counter() - pair_started,
        )

    def _extract_scene(self, archive_path: Path) -> SceneContext:
        """Читает метаданные и извлекает только требуемые каналы архива."""
        archive = SentinelArchive(archive_path)
        scene = SceneContext.from_zip_info(
            archive_path,
            archive.metadata,
            band_offsets=archive.read_band_offsets(),
        )
        required_bands = scene.level.required_bands
        if self.ndvi_only:
            required_bands = ("B04", "B08")
            if scene.level is ProductLevel.L2A:
                required_bands += ("SCL",)
        extracted_path = archive.extract(
            self.temporary_root,
            required_bands,
        )
        self.logger.info(
            "EXTRACT OK: %s → %s",
            archive_path.name,
            extracted_path,
        )
        return scene

    def _process_tile(self, scene: SceneContext) -> None:
        """Создаёт tile-level продукты одной сцены."""
        path_type = (
            L1CProductPaths
            if scene.level is ProductLevel.L1C
            else L2AProductPaths
        )
        TileImageProcessor(
            scene,
            path_type(scene, self.workspace),
            self.output_archive,
            self.options,
            products=self.products,
        ).run()

    def _crop_agroids(self, scene: SceneContext) -> None:
        """Обрезает продукты одной сцены по хозяйствам её тайла."""
        AgroCropProcessor(
            scene,
            SentinelCropPaths(scene, self.workspace),
            self.field_data,
            self.options,
            products=self.products,
        ).run()

    def _combine(self, scene: SceneContext) -> None:
        """Один раз объединяет фрагменты хозяйства на границе тайлов."""
        MosaicProcessor(
            scene,
            MosaicPaths(scene, self.workspace),
            products=self.products,
        ).run()

    def _apply_cloud_mask(self, scene: SceneContext) -> None:
        """Готовит облачную маску для статистики, не меняя визуальный NDVI."""
        if scene.level is ProductLevel.L1C:
            return
        paths = CloudMaskPaths(scene, self.workspace)
        RescaleSCLProcessor(scene, paths).run()
        FilterNDVIProcessor(scene, paths, self.options).run()

    def _collect_statistics(self, scene: SceneContext) -> None:
        """Рассчитывает статистику всех хозяйств согласованной пары."""
        NdviStatisticsProcessor(
            scene,
            NdviStatisticsPaths(scene, self.workspace),
            self.field_data,
            self.geometry_exporter,
            self.options.nodata,
            overwrite=self.overwrite_statistics,
        ).run()

    @staticmethod
    def _build_final_scene(
            pair: ArchivePair,
            scenes: list[SceneContext],
    ) -> SceneContext:
        """Проверяет пару и создаёт контекст всех её хозяйств."""
        if len(scenes) != 2:
            raise ValueError("Для обработки требуется ровно два тайла")
        first, second = scenes
        if (
                first.acquired_on != second.acquired_on
                or first.satellite != second.satellite
                or first.level is not second.level
        ):
            raise ValueError("Архивы пары относятся к разным съёмкам или уровням")
        if first.acquired_on != pair.acquired_on or first.level is not pair.level:
            raise ValueError("Метаданные архивов не соответствуют найденной паре")
        agroids = tuple(dict.fromkeys((*first.agroids, *second.agroids)))
        return replace(first, agroids=agroids)

    def _run_step(
            self,
            name: str,
            context: str,
            handler: Callable[[], object],
    ):
        """Измеряет этап и добавляет его имя к диагностике ошибки."""
        started = perf_counter()
        self.logger.info("STEP START: %s | %s", name, context)
        try:
            result = handler()
        except Exception as exc:
            self.logger.exception(
                "STEP FAIL: %s | %s | %.2f сек. | %s",
                name,
                context,
                perf_counter() - started,
                exc,
            )
            raise ProcessingStepError(name, str(exc)) from exc
        self.logger.info(
            "STEP OK: %s | %s | %.2f сек.",
            name,
            context,
            perf_counter() - started,
        )
        return result

    @staticmethod
    def _scene_label(scene: SceneContext) -> str:
        """Формирует компактную метку сцены для журнала."""
        return (
            f"{scene.satellite}/{scene.tile}/"
            f"{scene.acquired_on.isoformat()}/{scene.level.value}"
        )
