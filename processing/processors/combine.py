"""Класс для соединения спутниковых изображений."""
import os

from osgeo import gdal

from core.logging import get_logger
from processing.dataset import atomic_raster_path
from processing.domain import ProductLevel


class MosaicProcessor:
    """
    Универсальный класс для объединения готовых TIFF‑тайлов
    (для agroid=1 и любых других сценариев).
    """

    PRODUCT_SIZES = {
        "tci": 10,
        "ndvi": 10,
        "ndwi": 10,
        "scl": 20,
    }

    def __init__(self, scene, paths, products=None):
        self.scene = scene
        self.paths = paths
        self.logger = get_logger(self.__class__.__name__)
        self.products = frozenset(products or self.PRODUCT_SIZES)
        unknown = self.products - self.PRODUCT_SIZES.keys()
        if unknown:
            raise ValueError(
                "Неизвестные продукты мозаики: "
                + ", ".join(sorted(unknown))
            )

    def run(self) -> None:
        """Объединяет доступные пары тайлов каждого продукта в GeoTIFF."""
        products = [
            product
            for product in ("tci", "ndvi", "ndwi", "scl")
            if product in self.products and not (
                product == "scl"
                and self.scene.level is ProductLevel.L1C
            )
        ]

        for prod in products:
            size = self.PRODUCT_SIZES[prod]
            tiles = self.paths.sources(prod)

            if len(tiles) < 2:
                self.logger.info(
                    "[%s] найдено %s тайлов за %s → пропуск",
                    prod, len(tiles), self.scene.date_label
                )
                continue

            dst = self.paths.destination(prod)

            if os.path.exists(dst):
                self.logger.info("[%s] %s уже есть → пропуск", prod, dst)
                continue

            self.logger.info(
                "[%s] объединяем %s тайлов → %s", prod, len(tiles), dst
            )
            vrt_path = (
                f"/vsimem/{self.scene.satellite}_"
                f"{self.scene.date_label}_{prod}_combine.vrt"
            )
            vrt = gdal.BuildVRT(vrt_path, tiles)
            if not vrt:
                raise RuntimeError(
                    f"Не удалось собрать VRT для {prod} "
                    f"за {self.scene.date_label}"
                )
            try:
                with atomic_raster_path(dst) as temporary:
                    result = gdal.Translate(
                        temporary,
                        vrt,
                        xRes=size,
                        yRes=size,
                        format="GTiff",
                        creationOptions=[
                            "TILED=YES",
                            "BIGTIFF=IF_SAFER",
                        ],
                    )
                    if result is None:
                        raise RuntimeError(
                            f"Не удалось объединить {prod} "
                            f"для {self.scene.date_label}"
                        )
                    result.FlushCache()
                    result = None
            finally:
                vrt = None
                gdal.Unlink(vrt_path)
            self.logger.info("[%s] успешно объединено → %s", prod, dst)
