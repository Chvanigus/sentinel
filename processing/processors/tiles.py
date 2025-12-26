"""Класс для работы с тайлами."""
import os

from glob2 import glob

from core import settings
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
            if not src:
                self.logger.error(f"Не найден исходник для {stage.upper()}")
                continue

            dst = self.pm.get_destination(stage=stage)
            if os.path.exists(dst):
                self.logger.info(f"{stage.upper()} уже есть, пропуск")
                continue

            RastrProcessing(src, dst_path=dst).projection_raster(dst_path=dst)
            self.logger.info(f"{stage.upper()} готово: {dst}")

    def _process_index_stage(
            self,
            stage: str,
            band1: str,
            band2: str,
            creator: callable
    ):
        """Общий метод для NDVI/NDWI: берём два JP2, создаём индекс."""
        jp2_1 = self._get_first_source(band1)
        jp2_2 = self._get_first_source(band2)
        if not jp2_1 or not jp2_2:
            self.logger.error(
                f"Не найдены {band1.upper()}/{band2.upper()} для {stage.upper()}")
            return

        dst = self.pm.get_destination(stage=stage)
        if os.path.exists(dst):
            self.logger.info(f"{stage.upper()} уже есть, пропуск")
            return

        idx = IndexProcessing(
            output_file=dst,
            **{f"{band1}_file": jp2_1, f"{band2}_file": jp2_2}
        )
        creator(idx)
        self.logger.info(f"{stage.upper()} готово: {dst}")

    def _get_first_source(self, stage: str) -> str:
        """
        Возвращает первый путь из self.pm.get_sources
        или None, если список пуст.
        """
        sources = self.pm.get_sources(stage=stage)
        return sources[0] if sources else None


class TilePathManager(BasePathManager):
    def get_sources(self, stage, agroid=None):
        base = settings.TEMP_PROCESSING_DIR
        bands = {
            'tci': 'TCI',
            'scl': 'SCL',
            'b03': 'B03',
            'b04': 'B04',
            'b08': 'B08',
        }.get(stage)
        if not bands:
            return []
        pattern = os.path.join(
            base,
            f"{self.satellite.upper()}_MSIL2A*{self.tile}*",
            "GRANULE", f"L2?_{self.tile}*", "IMG_DATA",
            f"R{20 if stage == 'scl' else 10}m",
            f"{self.tile}*{bands}_{20 if stage == 'scl' else 10}m.jp2"
        )
        files = glob(pattern)
        return files

    def get_destination(self, stage, agroid=None):
        out = settings.INTERMEDIATE
        name = f"{self.satellite}_{self.tile}_{self.date}_{stage}_3857.tif"
        return os.path.join(out, name)


def execute_tile_image_processor(**kwargs) -> None:
    """
    Вызов класса обработки архива со спутниковыми изображениями L2A уровня.
    :param kwargs: Дополнительные параметры для обработки архива.
    """
    pm = TilePathManager(**kwargs)
    processor = TileImageProcessor(**kwargs, path_manager=pm)
    processor.execute()
