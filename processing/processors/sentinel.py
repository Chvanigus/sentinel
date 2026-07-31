"""Класс для нарезания спутниковых снимков по агропредприятиями."""
from __future__ import annotations

import os

from osgeo import gdal, osr

from processing.dataset import atomic_raster_path, open_raster
from processing.domain import ProductLevel
from processing.geometry import intersect_raster_bounds
from processing.ports import FieldDataProvider
from processing.processors.base import BaseImageProcessor


class AgroCropProcessor(BaseImageProcessor):
    """Вырезает продукты сцены по границам связанных хозяйств."""

    def __init__(
            self,
            scene,
            paths,
            field_data: FieldDataProvider,
            options,
    ):
        super().__init__(scene, paths)
        self.field_data = field_data
        self.options = options
        self._agro_bounds_cache: dict[
            int,
            tuple[float, float, float, float],
        ] = {}
        self._crop_bounds_cache: dict[
            int,
            tuple[float, float, float, float] | None,
        ] = {}

    def run(self) -> None:
        """Обрабатывает все допустимые продукты каждого хозяйства сцены."""
        warp_keys = ["tci", "ndvi", "ndwi"]
        if self.scene.level is ProductLevel.L2A:
            warp_keys.append("scl")

        sources = {}
        for stage in warp_keys:
            stage_sources = self.paths.sources(stage)
            if not stage_sources:
                raise FileNotFoundError(
                    f"{stage}: исходники не найдены"
                )
            sources[stage] = stage_sources[0]

        for agroid in self.scene.agroids:
            for stage in warp_keys:
                self.logger.info(
                    "%s_a%s_%s - обработка %s",
                    stage,
                    agroid,
                    self.scene.date_label,
                    self.scene.level.value,
                )
                self._process_stage(stage, agroid, sources[stage])

    def _process_stage(
            self,
            stage: str,
            agroid: int,
            src: str,
    ) -> None:
        """Запускает пространственную вырезку заранее найденного продукта."""
        dst = self.paths.destination(stage, agroid)

        if os.path.exists(dst):
            self.logger.info("%s уже есть — пропуск", dst)
            return

        self._warp(src, dst, agroid, stage)

    def _get_bounds(
            self,
            agroid: int,
            src_file: str,
    ) -> tuple[float, float, float, float] | None:
        """
        Берём границы из PostGIS и приводим к координатам снимка.
        """
        if agroid in self._crop_bounds_cache:
            return self._crop_bounds_cache[agroid]
        if agroid not in self._agro_bounds_cache:
            self._agro_bounds_cache[agroid] = self.field_data.bounds(
                year=self.scene.acquired_on.year,
                agroid=agroid,
                srid=self.options.destination_srid,
            )
        raw = self._agro_bounds_cache[agroid]

        fixed = intersect_raster_bounds(
            raw,
            src_file,
            self.options.destination_srid,
        )

        if fixed[0] >= fixed[2] or fixed[1] >= fixed[3]:
            self.logger.warning("Агро %s: зона вне кадра → пропуск", agroid)
            self._crop_bounds_cache[agroid] = None
            return None
        self._crop_bounds_cache[agroid] = fixed
        return fixed

    def _warp(
            self,
            src: str,
            dst: str,
            agroid: int,
            stage: str,
    ) -> None:
        """Вырезает продукт, не интерполируя категориальную SCL-маску."""
        bounds = self._get_bounds(agroid, src)
        if not bounds:
            return

        with open_raster(src) as ds:
            src_srs = osr.SpatialReference(wkt=ds.GetProjection())
            dst_srs = osr.SpatialReference()
            dst_srs.ImportFromEPSG(self.options.destination_srid)
            res = ds.GetGeoTransform()[1]
            resample_algorithm = (
                gdal.GRA_NearestNeighbour
                if stage == "scl"
                else gdal.GRA_Lanczos
            )

            with atomic_raster_path(dst) as temporary:
                result = gdal.Warp(
                    temporary,
                    ds,
                    format="GTiff",
                    outputBounds=bounds,
                    outputBoundsSRS=dst_srs,
                    srcSRS=src_srs,
                    dstSRS=dst_srs,
                    xRes=res,
                    yRes=res,
                    resampleAlg=resample_algorithm,
                    srcNodata=self.options.nodata,
                    dstNodata=self.options.nodata,
                    multithread=True,
                    warpOptions=[
                        "NUM_THREADS=ALL_CPUS",
                        "INIT_DEST=NO_DATA",
                    ],
                    creationOptions=["TILED=YES", "BIGTIFF=IF_SAFER"],
                )
                if result is None:
                    raise RuntimeError(
                        f"Не удалось создать {dst} для агро {agroid}"
                    )
                result.FlushCache()
                result = None
        self.logger.info("Нарезка для агро %s готова: %s", agroid, dst)
