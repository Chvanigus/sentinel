"""Обработка архивов со спутниковыми снимками."""
import os

from core import settings
from core.zip.handlers import ZipHandler
from processing.processors.base import BaseImageProcessor


class ZipFilesProcessor(BaseImageProcessor):
    """
    Класс, который отвечает за взаимодействие и обработку спутниковых
    снимков из архива.
    """

    def __init__(self, tile: str, date: str, satellite: str, file: str):
        super().__init__(tile, date, satellite, path_manager=None)
        self.file = file
        self.satellite = satellite.upper()

    def _process_files(self):
        """Разархивирует zip файл в директорию "temp" для работы."""
        zh = ZipHandler(self.file)
        z_name = zh.get_basename()
        self.logger.info(f"Начата обработка архива {z_name}")

        path = os.path.join(settings.TEMP_PROCESSING_DIR, z_name)
        if os.path.exists(path):
            self.logger.info(f"Архив {z_name} уже распакован. Пропуск.")
            return

        zh.unzip(settings.TEMP_PROCESSING_DIR)

        self.logger.info(f"Архив {z_name} успешно разархивирован.")


def execute_zip_files_processor(zip_file: str, **kwargs) -> None:
    """
    Вызов класса обработки архива со спутниковыми изображениями.
    :param zip_file: Путь к ZIP файлу со спутниковыми изображениями.
    :param kwargs: Дополнительные параметры для обработки архива.
    """
    processor = ZipFilesProcessor(file=zip_file, **kwargs)
    processor.execute()
