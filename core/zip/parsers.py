"""Парсер названия архива."""
import re
from collections import namedtuple
from datetime import datetime


class ZipParser:
    """Класс для парсинга названия архива спутникового снимка."""
    def __init__(self, filename: str):
        self.filename = filename
        self.parts = self._parse_filename()
        self.Info = namedtuple(
            typename="ZipInfo", 
            field_names=["satellite", "date", "tile", "level"]
        )

    def get_basename_without_extension(self) -> str:
        """Возвращает название архива без расширения."""
        return self.filename.split(".")[0]

    def _parse_filename(self) -> tuple:
        """
        Парсер названия файла.
        :return: Возвращает кортеж из названия спутника, даты и квадрата
        """
        # Название спутника
        satellite = re.match(r"S[1-9][A-Z]", self.filename).group()

        # Дата снимка
        date = datetime.strptime(
            re.search(r"20\d{6}", self.filename).group(), "%Y%m%d"
        ).date()

        # Название сцены
        tile = re.search(r"[A-Z]\d{2}[A-Z]{3}", self.filename).group()

        level = re.search(r"MSIL[1-2][A-C]", self.filename).group()

        return satellite, date, tile, level

    def get_info(self):
        """Возвращает namedtuple для данных из архива."""
        return self.Info(*self.parts)
