"""Обработка согласованной пары соседних архивов Sentinel."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from time import perf_counter

from core.logging import get_logger

from .archive import SentinelArchive
from .domain import AGROIDS_BY_TILE, ArchivePair, ProductLevel, SceneContext
from .exceptions import ProcessingStepError
from .paths import (
    CloudMaskPaths,
    L1CProductPaths,
    L2AProductPaths,
    MosaicPaths,
    NdviStatisticsPaths,
    SentinelCropPaths,
)
from .processors.cloudmask import RescaleSCLProcessor
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
            geometry_exporter,
            products=None,
            ndvi_only: bool = False,
            overwrite_statistics: bool = False,
    ) -> None:
        self.temporary_root = Path(temporary_root)
        self.workspace = workspace
        self.options = options
        self.field_data = field_data
        self.geometry_exporter = geometry_exporter
        self.products = products
        self.ndvi_only = ndvi_only
        self.overwrite_statistics = overwrite_statistics
        self.logger = get_logger(self.__class__.__name__)

    def process(
            self,
            pair: ArchivePair,
            target_agroids: tuple[int, ...] | None = None,
    ) -> None:
        """Обрабатывает только хозяйства, отсутствующие у выбранной даты."""
        pair_started = perf_counter()
        scenes = []
        targets = set(target_agroids) if target_agroids is not None else None
        for side, archive_path in zip(
                ("ula", "ulb"),
                pair.archives,
                strict=True,
        ):
            expected_tile = f"{pair.prefix}{side}".lower()
            if targets is not None and targets.isdisjoint(
                    AGROIDS_BY_TILE.get(expected_tile, ())
            ):
                self.logger.info(
                    "TILE SKIP: %s → целевые хозяйства отсутствуют",
                    expected_tile,
                )
                continue
            archive, scene = self._run_step(
                "inspect",
                archive_path.name,
                lambda path=archive_path: self._inspect_scene(path),
            )
            if targets is not None:
                scene = replace(
                    scene,
                    agroids=tuple(
                        agroid
                        for agroid in scene.agroids
                        if agroid in targets
                    ),
                )
            self._run_step(
                "extract",
                archive_path.name,
                lambda current_archive=archive, current=scene: (
                    self._extract_bands(current_archive, current)
                ),
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
            "scl-rescale",
            self._scene_label(final_scene),
            lambda: self._prepare_scl(final_scene),
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

    def _inspect_scene(
            self,
            archive_path: Path,
    ) -> tuple[SentinelArchive, SceneContext]:
        """Читает метаданные архива без извлечения тяжёлых каналов."""
        archive = SentinelArchive(archive_path)
        scene = SceneContext.from_zip_info(
            archive_path,
            archive.metadata,
            band_offsets=archive.read_band_offsets(),
        )
        return archive, scene

    def _extract_bands(
            self,
            archive: SentinelArchive,
            scene: SceneContext,
    ) -> None:
        """Извлекает минимальный набор каналов ещё не кешированной сцены."""
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
            scene.archive_path.name,
            extracted_path,
        )

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
        if 1 not in scene.agroids:
            return
        MosaicProcessor(
            scene,
            MosaicPaths(scene, self.workspace),
            products=self.products,
        ).run()

    def _prepare_scl(self, scene: SceneContext) -> None:
        """Приводит SCL к сетке NDVI без создания фильтрованной копии."""
        if scene.level is ProductLevel.L1C:
            return
        paths = CloudMaskPaths(scene, self.workspace)
        RescaleSCLProcessor(scene, paths).run()

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
        if not scenes:
            raise ValueError("Для обработки не выбрано ни одного хозяйства")
        first = scenes[0]
        for scene in scenes:
            if (
                    scene.acquired_on != first.acquired_on
                    or scene.satellite != first.satellite
                    or scene.level is not first.level
            ):
                raise ValueError(
                    "Архивы пары относятся к разным съёмкам или уровням"
                )
            if (
                    scene.acquired_on != pair.acquired_on
                    or scene.level is not pair.level
            ):
                raise ValueError(
                    "Метаданные архивов не соответствуют найденной паре"
                )
        agroids = tuple(dict.fromkeys(
            agroid
            for scene in scenes
            for agroid in scene.agroids
        ))
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
