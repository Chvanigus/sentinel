"""Класс для обработки спутниковых снимков с помощью маски облачности."""
import os

import numpy as np
from osgeo import gdal

from core import settings
from processing.dataset import GDALDatasetProcessing
from processing.processors.base import BaseImageProcessor, BasePathManager


class RescaleSCLProcessor(BaseImageProcessor):
    """Ресемплирует маску облачности (SCL) до 10м, основываясь на разрешении NDVI."""

    def __init__(self, tile, date, satellite, agroids, path_manager):
        super().__init__(tile, date, satellite, path_manager)
        self.agroids = agroids

    def _process_files(self):
        for agroid in self.agroids:
            ndvi_path = self.pm.get_sources(stage="ndvi", agroid=agroid)[0]
            if not os.path.exists(ndvi_path): continue

            scl_src = self.pm.get_sources(stage="scl_20", agroid=agroid)[0]
            scl_dst = self.pm.get_destination(stage="scl_10", agroid=agroid)

            if os.path.exists(scl_dst):
                self.logger.info(
                    f"SCL_10m для агро {agroid} уже есть → пропуск")
                continue

            ndvi_ds = gdal.Open(ndvi_path)
            scl_ds = gdal.Open(scl_src)

            xres = ndvi_ds.GetGeoTransform()[1]
            yres = ndvi_ds.GetGeoTransform()[5]

            gdal.Warp(
                scl_dst, scl_ds,
                xRes=xres, yRes=yres,
                resampleAlg=gdal.GRA_Bilinear
            )
            self.logger.info(f"SCL ресемплирован до 10м: {scl_dst}")


class FilterNDVIProcessor(BaseImageProcessor):
    """Фильтрует NDVI с использованием SCL-маски (10м)."""
    VALID_SCL_VALUES = [4, 5, 6, 7]

    def __init__(self, tile, date, satellite, agroids, path_manager):
        super().__init__(tile, date, satellite, path_manager)
        self.agroids = agroids

    def _process_files(self, *_):
        for agroid in self.agroids:
            ndvi_path = self.pm.get_sources(stage="ndvi", agroid=agroid)[0]
            if not os.path.exists(ndvi_path): continue

            scl_path = self.pm.get_sources(stage="scl_10", agroid=agroid)[0]
            dst_ndvi = self.pm.get_destination(stage="ndvi_filtered",
                                               agroid=agroid)

            if os.path.exists(dst_ndvi):
                self.logger.info(
                    f"Фильтрованный NDVI уже есть для агро {agroid} → пропуск")
                continue

            scl_ds = gdal.Open(scl_path)
            scl_array = scl_ds.GetRasterBand(1).ReadAsArray()

            base = settings.TEMP_PROCESSING_DIR
            tmp_resampled_ndvi = os.path.join(
                base, f"res_ndvi_a{agroid}.tif"
            )

            gdal.Warp(tmp_resampled_ndvi, ndvi_path,
                      width=scl_ds.RasterXSize,
                      height=scl_ds.RasterYSize,
                      outputBounds=self._get_bounds_from_ds(scl_ds),
                      resampleAlg=gdal.GRA_Bilinear,
                      dstSRS=scl_ds.GetProjection())

            # Чтение временного массива
            ds = gdal.Open(tmp_resampled_ndvi)
            band = ds.GetRasterBand(1)
            ndvi_array = band.ReadAsArray()

            mask = np.isin(scl_array, self.VALID_SCL_VALUES)

            filtered = np.where(mask, ndvi_array, settings.NODATA)

            GDALDatasetProcessing(
                scl_path, dst_ndvi, filtered
            ).create_file_from_array()

            self.logger.info(f"NDVI отфильтрован для агро {agroid}")

    @staticmethod
    def _get_bounds_from_ds(ds):
        gt = ds.GetGeoTransform()
        return (gt[0], gt[3] + gt[5] * ds.RasterYSize,
                gt[0] + gt[1] * ds.RasterXSize, gt[3])


class CloudPathManager(BasePathManager):
    def get_sources(self, stage, agroid=None):
        base = settings.INTERMEDIATE
        stage_map = {
            "ndvi": f"{self.satellite}_{self.date}_a{agroid}_ndvi_10m_3857.tif",
            "scl_20": f"{self.satellite}_{self.date}_a{agroid}_scl_20m_3857.tif",
            "scl_10": f"{self.satellite}_{self.date}_a{agroid}_scl_10m_3857.tif",
        }
        return [os.path.join(base, stage_map[stage])]

    def get_destination(self, stage, agroid=None) -> str:
        base = settings.INTERMEDIATE
        paths = {
            "scl_10": f"{self.satellite}_{self.date}_a{agroid}_scl_10m_3857.tif",
            "ndvi_filtered": f"{self.satellite}_{self.date}_a{agroid}_ndvi_10m_3857_filtered.tif"
        }
        return os.path.join(base, paths.get(stage))


def execute_cloud_mask_image_processor(agroids: list, **kwargs) -> None:
    """
    Вызов класса для создания маски облачности.
    :param agroids: Список агропредприятий для обработки.
    :param kwargs: Дополнительные параметры для обработки снимков.
    """
    pm = CloudPathManager(**kwargs)

    rescaler = RescaleSCLProcessor(agroids=agroids, path_manager=pm, **kwargs)
    rescaler.execute()

    filterer = FilterNDVIProcessor(agroids=agroids, path_manager=pm, **kwargs)
    filterer.execute()
