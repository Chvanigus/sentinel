"""Класс для обработки спутниковых снимков с помощью маски облачности."""
import os

from osgeo import gdal

from core.logging import get_logger
from processing.dataset import (
    atomic_raster_path,
    open_raster,
)


class RescaleSCLProcessor:
    """
    Ресемплирует маску облачности (SCL) до 10м,
    основываясь на разрешении NDVI.
    """

    def __init__(self, scene, paths):
        self.scene = scene
        self.paths = paths
        self.logger = get_logger(self.__class__.__name__)

    def run(self) -> None:
        """Ресемплирует SCL каждого хозяйства до сетки соответствующего NDVI."""
        for agroid in self.scene.agroids:
            ndvi_path = self.paths.ndvi(agroid)
            if not os.path.exists(ndvi_path):
                self.logger.warning("NDVI не найден: %s", ndvi_path)
                continue

            scl_src = self.paths.scl_20m(agroid)
            if not os.path.exists(scl_src):
                self.logger.warning("SCL не найден: %s", scl_src)
                continue
            scl_dst = self.paths.scl_10m(agroid)

            if os.path.exists(scl_dst):
                self.logger.info(
                    "SCL_10m для агро %s уже есть → пропуск", agroid
                )
                continue

            with open_raster(ndvi_path) as ndvi_ds, open_raster(
                    scl_src
            ) as scl_ds:
                with atomic_raster_path(scl_dst) as temporary:
                    result = gdal.Warp(
                        temporary,
                        scl_ds,
                        format="GTiff",
                        width=ndvi_ds.RasterXSize,
                        height=ndvi_ds.RasterYSize,
                        outputBounds=self._get_bounds_from_ds(ndvi_ds),
                        outputBoundsSRS=ndvi_ds.GetProjection(),
                        dstSRS=ndvi_ds.GetProjection(),
                        resampleAlg=gdal.GRA_NearestNeighbour,
                        multithread=True,
                        warpOptions=["NUM_THREADS=ALL_CPUS"],
                        creationOptions=[
                            "TILED=YES",
                            "COMPRESS=DEFLATE",
                            "PREDICTOR=2",
                            "NUM_THREADS=ALL_CPUS",
                        ],
                    )
                    if result is None:
                        raise RuntimeError(
                            "Не удалось ресемплировать SCL "
                            f"для агро {agroid}"
                        )
                    result.FlushCache()
                    result = None
            self.logger.info("SCL ресемплирован до 10м: %s", scl_dst)

    @staticmethod
    def _get_bounds_from_ds(ds):
        """Вычисляет границы GDAL dataset по его геотрансформации."""
        gt = ds.GetGeoTransform()
        return (
            gt[0],
            gt[3] + gt[5] * ds.RasterYSize,
            gt[0] + gt[1] * ds.RasterXSize,
            gt[3],
        )
