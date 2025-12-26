"""Класс для соединения спутниковых изображений."""
import os
from typing import List

from glob2 import glob
from osgeo import gdal

from core import settings, const
from processing.processors.base import BaseImageProcessor, BasePathManager


class CombineImageProcessor(BaseImageProcessor):
    """
    Универсальный класс для объединения готовых TIFF‑тайлов
    (для agroid=1 и любых других сценариев).
    """

    def _process_files(self):
        products = ["tci", "scl", "ndvi", "ndwi"]
        stage_cfg = {
            "tci": 10,
            "scl": 20,
            "ndvi": 10,
            "ndwi": 10,
        }

        for prod in products:
            size = stage_cfg[prod]
            pattern = os.path.join(
                settings.INTERMEDIATE,
                f"*_{self.date}_a1_{prod}_{size}m_3857*.tif"
            )
            tiles: List[str] = sorted(glob(pattern))

            if len(tiles) < 2:
                self.logger.info(
                    f"[{prod}] найдено {len(tiles)} тайлов за {self.date} → пропуск")
                continue

            dst = self.pm.get_destination(stage=prod)

            if os.path.exists(dst):
                self.logger.info(f"[{prod}] {dst} уже есть → пропуск")
                continue

            self.logger.info(
                f"[{prod}] объединяем {len(tiles)} тайлов → {dst}")
            vrt = gdal.BuildVRT("/vsimem/temp_combine.vrt", tiles)
            if not vrt:
                self.logger.error(f"[{prod}] не удалось собрать VRT")
                continue

            gdal.Translate(dst, vrt, xRes=size, yRes=size,
                           format=const.FORMAT_GEOTIFF)
            self.logger.info(f"[{prod}] успешно объединено → {dst}")

    @staticmethod
    def _extract_tile(path: str) -> str:
        """
        Парсит tile из пути. Предполагаем, что он в конце имени перед '_stage'.
        Пример: s2a_10_07_2024_a1_tci_10m_3857_T38ULA.tif → вернёт T38ULA
        """
        base = os.path.basename(path)
        parts = base.split("_")
        if len(parts) >= 2:
            last = parts[-1]
            if last.endswith(".tif") and len(last) > 4:
                tile = last[:-4]  # обрезаем .tif
                return tile
        return "unknown_tile"


class CombinePathManager(BasePathManager):
    STAGE_SIZES = {
        "tci": 10,
        "scl": 20,
        "ndvi": 10,
        "ndwi": 10,
    }

    def get_sources(self, stage, agroid=None):
        """
        Ищем все тайлы за дату и stage с agroid=1 в INTERMEDIATE.
        """
        size = self.STAGE_SIZES.get(stage, 10)
        pattern = os.path.join(
            settings.INTERMEDIATE,
            f"*_{self.date}_a{agroid}_{stage}_{size}m_*.tif"
        )
        return sorted(glob(pattern))

    def get_destination(self, stage, agroid=1):
        """
        Строим путь итогового объединённого изображения:
        - TCI → включаем tile в имя
        - остальное → нет
        """
        size = self.STAGE_SIZES.get(stage, 10)
        name = f"{self.satellite}_{self.date}_a1_{stage}_{size}m_3857.tif"

        if stage == "tci":
            base = settings.PROCESSED_DIR
        else:
            base = settings.INTERMEDIATE

        return os.path.join(base, name)


def execute_combine_image_processor(**kwargs) -> None:
    """
    Вызов класса для объединения изображений по сценам.
    :param kwargs: Дополнительные параметры для обработки снимков.
    """
    pm = CombinePathManager(**kwargs)
    processor = CombineImageProcessor(**kwargs, path_manager=pm)
    processor.execute()
