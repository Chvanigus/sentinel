"""Класс для работы с тайлами."""
import os
import shutil

from glob2 import glob

from core import settings
from core.utils import get_basename
from processing.indexes import IndexProcessing
from processing.processors.base import BaseImageProcessor, BasePathManager
from processing.rastr import RastrProcessing


class TileImageProcessor(BaseImageProcessor):
    def _process_files(self):
        # 1) растерные этапы: TCI и SCL
        self._process_raster_stages()

        # 2) индексные этапы: NDVI и NDWI
        self._process_index_stage(
            stage="ndvi",
            band1="b04",
            band2="b08",
            creator=IndexProcessing.create_ndvi_image
        )
        self._process_index_stage(
            stage="ndwi",
            band1="b03",
            band2="b08",
            creator=IndexProcessing.create_ndwi_image
        )

    def _process_raster_stages(self):
        """Подготовка TCI (10m) и SCL (20m) из JP2 → projection_raster."""
        for stage in ("tci", "scl"):
            src = self._get_first_source(stage)
            if src is None:
                return
            self.logger.info(
                "Обработка изображения %s (%s)",
                get_basename(src), stage.upper()
            )
            if not src:
                self.logger.warning(
                    "Не найден исходник для %s, пропуск", stage.upper()
                )
                continue

            dst = self.pm.get_destination(stage=stage)
            if os.path.exists(dst):
                self.logger.info("%s уже есть, пропуск", stage.upper())
                continue

            RastrProcessing(src, dst_path=dst).projection_raster(dst_path=dst)
            self.logger.info("%s готово: %s", stage.upper(), dst)

            self._copy_to_geoware(dst, stage)

    def _process_index_stage(
            self,
            stage: str,
            band1: str,
            band2: str,
            creator: callable
    ):
        """Общий метод для индексов: берём два JP2, создаём индекс."""
        jp2_1 = self._get_first_source(band1)
        jp2_2 = self._get_first_source(band2)
        if not jp2_1 or not jp2_2:
            self.logger.error(
                "Не найдены %s/%s для %s",
                band1.upper(), band2.upper(), stage.upper(),
            )
            return

        dst = self.pm.get_destination(stage=stage)
        self.logger.info(
            "Обработка индекса %s (%s) из бандов %s и %s",
            stage.upper(), get_basename(dst), band1.upper(), band2.upper()
        )
        if os.path.exists(dst):
            self.logger.info("%s уже есть, пропуск", stage.upper())
            return

        idx = IndexProcessing(
            output_file=dst,
            **{f"{band1}_file": jp2_1, f"{band2}_file": jp2_2}
        )
        creator(idx)
        self.logger.info("%s готово: %s", stage.upper(), dst)

        self._copy_to_geoware(dst, stage)

    def _get_first_source(self, stage: str) -> str:
        """
        Возвращает первый путь из self.pm.get_sources
        или None, если список пуст.
        """
        sources = self.pm.get_sources(stage=stage)
        return sources[0] if sources else None

    def _copy_to_geoware(self, src_path: str, img_type: str):
        """Копирует готовый файл в geoware по шаблону /<год>/<тайл>/<img_type>/<месяц>/"""
        if not os.path.exists(src_path):
            self.logger.warning("Файл не найден для копирования: %s", src_path)
            return

        tile_upper = self.tile.upper()
        day, month_str, year_str = self.date.split("_")
        year = int(year_str)
        month = int(month_str)

        dst_dir = os.path.join(
            "/mnt/map/geoware",
            str(year),
            tile_upper,
            img_type.lower(),
            f"{month:02d}"
        )
        os.makedirs(dst_dir, exist_ok=True)

        dst_path = os.path.join(dst_dir, os.path.basename(src_path))
        shutil.copy2(src_path, dst_path)
        self.logger.info("Файл скопирован в geoware: %s", dst_path)

        if getattr(self.pm, "level", "").lower() == "msil2a" and img_type.lower() != "scl":
            scl_sources = self.pm.get_sources("scl")
            for scl_src in scl_sources:
                scl_dst_dir = os.path.join(
                    "/mnt/map/geoware",
                    str(year),
                    tile_upper,
                    "scl",
                    f"{month:02d}"
                )
                os.makedirs(scl_dst_dir, exist_ok=True)
                scl_dst_path = os.path.join(scl_dst_dir,
                                            os.path.basename(scl_src))
                if os.path.exists(scl_src):
                    shutil.copy2(scl_src, scl_dst_path)
                    self.logger.info("SCL файл скопирован в geoware: %s",
                                     scl_dst_path)


class L2APathManager(BasePathManager):
    """PathManager для L2A обработки снимков."""
    def get_sources(self, stage, agroid=None):
        base = settings.TEMP_PROCESSING_DIR

        bands = {
            "tci": "TCI",
            "scl": "SCL",
            "b03": "B03",
            "b04": "B04",
            "b08": "B08",
        }.get(stage)

        if not bands:
            return []

        resolution = 20 if stage == "scl" else 10

        pattern = os.path.join(
            base,
            f"{self.satellite.upper()}_MSIL2A*{self.tile.upper()}*",
            "GRANULE",
            f"L2A_{self.tile.upper()}*",
            "IMG_DATA",
            f"R{resolution}m",
            f"{self.tile.upper()}*{bands}_{resolution}m.jp2",
        )
        return glob(pattern)

    def get_destination(self, stage, agroid=None):
        out = settings.INTERMEDIATE
        name = f"{self.satellite}_{self.tile}_{self.date}_{stage}_3857.tif"
        return os.path.join(out, name)


class L1CPathManager(BasePathManager):
    """PathManager для L1C обработки снимков."""
    def get_sources(self, stage, agroid=None):
        base = settings.TEMP_PROCESSING_DIR

        if stage == "scl":
            return []

        bands = {
            "tci": "TCI",
            "b03": "B03",
            "b04": "B04",
            "b08": "B08",
        }.get(stage)

        if not bands:
            return []

        pattern = os.path.join(
            base,
            f"{self.satellite.upper()}_MSIL1C*{self.tile.upper()}*",
            "GRANULE",
            f"L1C_{self.tile.upper()}*",
            "IMG_DATA",
            f"{self.tile.upper()}*_{bands}.jp2",
        )
        return glob(pattern)

    def get_destination(self, stage, agroid=None):
        out = settings.INTERMEDIATE
        name = f"{self.satellite}_{self.tile}_{self.date}_{stage}_3857.tif"
        return os.path.join(out, name)


def execute_tile_image_processor(**kwargs) -> None:
    """
    Вызов класса обработки архива со
    спутниковыми изображениями L2A или L1C уровня.
    :param kwargs: Дополнительные параметры для обработки архива.
    """
    level = kwargs.get("level")

    if level == "msil2a":
        pm = L2APathManager(**kwargs)
    elif level == "msil1c":
        pm = L1CPathManager(**kwargs)
    else:
        raise ValueError(f"Неизвестный уровень обработки: {level}")

    processor = TileImageProcessor(**kwargs, path_manager=pm)
    processor.execute()