"""Класс для нарезания спутниковых снимков по агропредприятиями."""
import os
from typing import List, Optional

import psycopg2
from glob2 import glob
from osgeo import gdal, osr

from core import settings, const
from core.utils import get_date_obj
from db.connect_data import DSL
from db.db_class import get_postgis_worker
from processing import CoordProcessing
from processing.dataset import GDALDatasetContextManager
from processing.processors.base import BaseImageProcessor, BasePathManager


class SentinelImageProcessor(BaseImageProcessor):
    def __init__(self,
                 tile: str,
                 date: str,
                 agroids: List[int],
                 satellite: str,
                 path_manager,
                 level: str = None):
        super().__init__(tile, date, satellite, path_manager, level)
        self.agroids = agroids
        self.date_obj = get_date_obj(self.date)

    def _process_files(self):
        warp_keys = ["tci", "ndvi", "ndwi"]
        if not self.level == "msil1c":
            warp_keys.append("scl")

        for agroid in self.agroids:
            for stage in warp_keys:
                self.logger.info(
                    "%s_a%s_%s - обработка %s",
                    stage, agroid, self.date, self.level
                )
                self._process_stage(stage, agroid)

    def _process_stage(self, stage: str, agroid: int):
        sources = self.pm.get_sources(stage=stage, agroid=agroid)
        if not sources:
            self.logger.warning("%s_a%s - исходники не найдены", stage, agroid)
            return

        src = sources[0]
        dst = self.pm.get_destination(stage=stage, agroid=agroid)

        if os.path.exists(dst):
            self.logger.info("%s уже есть — пропуск", dst)
            return

        self._warp(src, dst, agroid)

    def _get_bounds(self, agroid: int, src_file: str) -> Optional[List[float]]:
        """
        Берём границы из PostGIS и приводим к координатам снимка.
        """
        with psycopg2.connect(**DSL) as conn:
            pw = get_postgis_worker(conn)
            raw = pw.get_bounds_lats_lons(
                year=self.date_obj.year,
                agroid=agroid,
                dstype=settings.DESTSRID
            )

        fixed = CoordProcessing(
            bounds=raw, src_path=src_file
        ).find_band_bounds()

        if fixed[0] > fixed[2] or fixed[1] > fixed[3]:
            self.logger.warning(f"Агро %s: зона вне кадра → пропуск", agroid)
            return None
        return fixed

    def _warp(self, src: str, dst: str, agroid: int):
        """gdal.Warp—обёртка с Lanczos и nodata из settings."""
        bounds = self._get_bounds(agroid, src)
        if not bounds:
            return

        with GDALDatasetContextManager(src) as ds:
            src_srs = osr.SpatialReference(wkt=ds.GetProjection())
            dst_srs = osr.SpatialReference()
            dst_srs.ImportFromEPSG(settings.DESTSRID)
            res = ds.GetGeoTransform()[1]

            gdal.Warp(
                dst, ds, format=const.FORMAT_GEOTIFF,
                outputBounds=bounds, outputBoundsSRS=dst_srs,
                srcSRS=src_srs, dstSRS=dst_srs,
                xRes=res, yRes=res, resampleAlg=gdal.GRIORA_Lanczos,
                srcNodata=settings.NODATA, dstNodata=settings.NODATA
            )
        self.logger.info(f"Нарезка для агро {agroid} готова: {dst}")


class SentinelPathManager(BasePathManager):
    def get_sources(self, stage, agroid=None) -> List[str]:
        """
        Ищем исходные TIF-файлы по pattern для tci, ndvi и т.п.
        """
        pattern = f"{self.satellite}_{self.tile}_{self.date}_{stage}_3857.tif"

        base = settings.INTERMEDIATE
        path = os.path.join(base, pattern)

        files = [p for p in glob(path)]
        return sorted(files)

    def get_destination(self, stage, agroid=None) -> str:
        """
        Формируем путь назначения:
        {satellite}_{date}_a{agroid}_{stage}_{size}m_3857.tif
        """
        stage_cfg = {
            "tci": 10,
            "ndvi": 10,
            "ndwi": 10,
        }
        if self.level == "msil2a":
            stage_cfg["scl"] = 20

        size = stage_cfg.get(stage, 10)

        if agroid == 1 or size == 20:
            base = settings.INTERMEDIATE
        else:
            base = settings.PROCESSED_DIR


        name = (
            f"{self.satellite.lower()}_"
            f"{self.date}_a{agroid}_"
            f"{stage.replace('_tif', '')}_"
            f"{size}m_3857.tif"
        )

        if agroid == 1:
            name = name.replace(".tif", f"_{self.tile}.tif")

        return os.path.join(base, name)


def execute_sentinel_image_processor(agroids: list, **kwargs) -> None:
    """
    Вызов класса обработки изображений по сценам.
    :param agroids: Список агропредприятий для обработки.
    :param kwargs: Дополнительные параметры для обработки снимков.
    """
    pm = SentinelPathManager(**kwargs)
    processor = SentinelImageProcessor(
        agroids=agroids, **kwargs, path_manager=pm
    )
    processor.execute()
