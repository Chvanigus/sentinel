"""Класс для окрашивания спутниковых изображений в RGB цвета."""
import os

from glob2 import glob

from core import settings
from processing.colormap import ColorMap
from processing.processors.base import BaseImageProcessor, BasePathManager


class ColorParseImageProcessor(BaseImageProcessor):
    """Класс для окрашивания спутниковых изображений в RGB цвета."""
    STAGES = ["ndvi", "ndwi", "scl"]

    def __init__(self, tile, date, satellite, agroids, path_manager) -> None:
        super().__init__(tile, date, satellite, path_manager)
        self.agroids = agroids

    def _process_files(self):
        for ag in self.agroids:
            self.logger.info(f"[ColorParse] Агро {ag}")
            for stage in self.STAGES:
                self._process_stage(stage, ag)

    def _process_stage(self, stage: str, agroid: int):
        src_list = self.pm.get_sources(stage=stage, agroid=agroid)
        if not src_list:
            self.logger.warning(
                f"[{stage}] для агро {agroid} нет исходника. Пропуск."
            )
            return

        src = src_list[0]
        if not os.path.exists(src):
            self.logger.warning(
                f"[{stage}] для агро {agroid }нет исходника. Пропуск"
            )
            return

        dst = self.pm.get_destination(stage=stage, agroid=agroid)
        if os.path.exists(dst):
            self.logger.debug(f"[{stage}] {os.path.basename(dst)} уже есть")
            return

        self.logger.info(
            f"[{stage}] окрашиваем {os.path.basename(src)} → {os.path.basename(dst)}"
        )

        cm = ColorMap(
            color_map_path=self.pm.get_color_map_path(stage),
            nodata=settings.NODATA
        )
        cm.parse_color_map()
        cm.create_rgba(src_path=src, dst_path=dst)
        self.logger.info(f"[{stage}] готово: {dst}")


class ColorPathManager(BasePathManager):
    """PathManager для ColorParseImageProcessor."""
    def get_sources(self, stage, agroid = None):
        pattern = (
            f"{self.satellite}_{self.date}_a{agroid}_{stage}_10m_3857.tif"
        )
        return sorted(glob(os.path.join(settings.INTERMEDIATE, pattern)))

    def get_destination(self, stage, agroid = None) -> str:
        name = (
            f"{self.satellite}_{self.date}_a{agroid}_{stage}_10m_3857_img.tif"
        )
        return os.path.join(settings.PROCESSED_DIR, name)

    @staticmethod
    def get_color_map_path(stage):
        """Возвращает путь к стилю окраски снимка."""
        return os.path.join(
            settings.STYLES_DIR, f"{stage}_style.qml"
        )



def execute_color_parse_image_processor(agroids: list, **kwargs) -> None:
    """
    Вызов класса для окрашивания спутниковых изображений в RGB цвета.
    :param agroids: Список агропредприятий для обработки.
    :param kwargs: Дополнительные параметры для обработки снимков.
    """
    pm = ColorPathManager(**kwargs)
    processor = ColorParseImageProcessor(
        agroids=agroids, **kwargs, path_manager=pm
    )
    processor.execute()
