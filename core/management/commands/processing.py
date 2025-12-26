"""Команда по полному циклу поиска и обработки изображений."""
import os
import sys

from glob2 import glob

from core import settings
from core.management.base import BaseCommand
from core.utils import remove_files_from_dir, copy_zip_to_archive
from core.zip.handlers import ZipHandler
from processing.processors import *
from processing.processors.tiles import execute_tile_image_processor
from satgeo.public import execute_publisher


class Command(BaseCommand):
    """Команда processing."""
    help = "Команда полного цикла обработки спутниковых изображений."

    def add_arguments(self, parser):
        parser.add_argument(
            "-d", "--debug", action="store_true",
            help="Режим разработчика. "
                 "В режиме разработки не удаляются файлы из отработанных папок"
        )

    def handle(self, *args, **options):
        self.run(debug=options["debug"])

    def run(self, debug: bool = False):
        zip_files = glob(os.path.join(settings.DOWNLOADS_DIR, '*.zip'))
        if not zip_files:
            self.logger.warning(
                "В директории download спутниковых снимков в формате '*.zip' "
                "не обнаружено. Поместите необходимые снимки в эту папку вручную."
            )
            sys.exit(-1)

        # Обрабатываем каждый снимок в цикле
        for zip_file in zip_files:
            self._process_zip(zip_file)

        # Публикация спутниковых снимков на Geoserver
        execute_publisher()

        # Удаление отработанных файлов
        if not debug:
            self._clean()

    @staticmethod
    def _process_zip(zip_file: str):
        """Полный цикл обработки архива со снимком."""
        zip_obj = ZipHandler(zip_file)
        info = zip_obj.get_zip_info()

        kwargs = {
            "date": info.date.strftime("%d_%m_%Y"),
            "tile": info.tile.lower(),
            "satellite": info.satellite.lower()
        }

        tile_name = info.tile[3:6].upper()
        year = info.date.year
        tile = kwargs["tile"]

        agroids = [1, 3, 4] if tile == "t38ula" else [1, 5, 6]

        copy_zip_to_archive(year, tile_name, zip_file)
        execute_zip_files_processor(zip_file=zip_file, **kwargs)

        execute_tile_image_processor(**kwargs)
        execute_sentinel_image_processor(agroids=agroids, **kwargs)
        execute_combine_image_processor(**kwargs)
        execute_cloud_mask_image_processor(agroids=agroids, **kwargs)
        execute_ndvi_statistics_image_processor(**kwargs)
        execute_color_parse_image_processor(agroids=agroids, **kwargs)

    @staticmethod
    def _clean():
        remove_files_from_dir(
            settings.INTERMEDIATE,
            settings.PROCESSED_DIR,
            settings.COMBINE_DIR,
            settings.NDVI_DIR,
            settings.TEMP_PROCESSING_DIR,
            settings.DOWNLOADS_DIR
        )
