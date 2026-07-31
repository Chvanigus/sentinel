"""Класс для обработки спутниковых снимков с помощью маски облачности."""
import os

from osgeo import gdal

from processing.calculations import apply_scl_mask
from processing.dataset import RasterArrayWriter, open_raster
from processing.processors.base import BaseImageProcessor


class RescaleSCLProcessor(BaseImageProcessor):
    """
    Ресемплирует маску облачности (SCL) до 10м,
    основываясь на разрешении NDVI.
    """

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
                result = gdal.Warp(
                    scl_dst,
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
                )
                if result is None:
                    raise RuntimeError(
                        f"Не удалось ресемплировать SCL для агро {agroid}"
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


class FilterNDVIProcessor(BaseImageProcessor):
    """Фильтрует NDVI с использованием SCL-маски (10м)."""
    VALID_SCL_VALUES = (4, 5, 6, 7)

    def __init__(self,
                 scene,
                 paths,
                 options):
        super().__init__(scene, paths)
        self.options = options

    def run(self) -> None:
        """Заменяет невалидные по SCL пиксели NDVI значением nodata."""
        for agroid in self.scene.agroids:
            ndvi_path = self.paths.ndvi(agroid)
            if not os.path.exists(ndvi_path):
                self.logger.warning("NDVI не найден: %s", ndvi_path)
                continue

            scl_path = self.paths.scl_10m(agroid)
            if not os.path.exists(scl_path):
                self.logger.warning("SCL 10m не найден: %s", scl_path)
                continue
            dst_ndvi = self.paths.filtered_ndvi(agroid)

            if os.path.exists(dst_ndvi):
                self.logger.info(
                    "Фильтрованный NDVI уже есть для агро %s → пропуск",
                    agroid
                )
                continue

            with open_raster(ndvi_path) as ndvi_ds, open_raster(
                    scl_path
            ) as scl_ds:
                ndvi_array = ndvi_ds.GetRasterBand(1).ReadAsArray()
                scl_array = scl_ds.GetRasterBand(1).ReadAsArray()

            filtered = apply_scl_mask(
                ndvi_array,
                scl_array,
                valid_classes=self.VALID_SCL_VALUES,
                nodata=self.options.nodata,
            )

            RasterArrayWriter(
                source=ndvi_path,
                destination=dst_ndvi,
                nodata=self.options.nodata,
            ).write(filtered)

            self.logger.info("NDVI отфильтрован для агро %s", agroid)
