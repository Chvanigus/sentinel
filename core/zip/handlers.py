"""Класс для работы с ZIP файлами."""
import sys
import zipfile
from collections import namedtuple

from core.logging import get_logger
from core.utils import get_basename
from core.zip.parsers import ZipParser


class ZipHandler:
    """Класс для работы с ZIP файлами."""

    def __init__(self, file: str):
        self.filename = file
        self.basename = self.get_basename()
        self.file = zipfile.ZipFile(self.filename)
        self.logger = get_logger()

    def get_basename(self) -> str:
        """Возвращает название архива без путей."""
        return get_basename(self.filename)

    def get_zip_info(self) -> namedtuple:
        """
        Возвращает данные по архиву.
        :return: Возвращает именованный кортеж данных по архиву
                 (satellite, date, tile)
        """
        return ZipParser(self.basename).get_info()

    def get_zip_name(self) -> str:
        """Возвращает название архива."""
        return self.basename

    def unzip(self, dst_path: str) -> None:
        """
        Распаковывает zip файл в нужную директорию.
        :param dst_path: Выходная директория данных из архива
        """
        try:
            self.file.extractall(dst_path)
        except zipfile.error as err:
            self.logger.error(
                f"Не удалось распаковать архив {self.filename}: {err}"
            )
            sys.exit(-1)
