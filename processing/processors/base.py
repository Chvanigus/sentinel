"""Базовые классы модуля processing."""
from abc import ABC, abstractmethod
from typing import List, Optional

from core.logging import get_logger


class BaseImageProcessor(ABC):
    """Базовый класс обработки изображений."""

    def __init__(self, tile: str, date: str, satellite: str, path_manager):
        self.tile = tile
        self.date = date
        self.satellite = satellite
        self.pm = path_manager
        self.logger = get_logger()

    @abstractmethod
    def _process_files(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        """Метод вызова класса обработки изображений."""
        self.logger.info(
            f"Запуск {self.__class__.__name__}({self.tile}, {self.date})"
        )
        self._process_files(*args, **kwargs)
        self.logger.info(f"Готово {self.__class__.__name__}")


class BasePathManager(ABC):
    """
    Отвечает за формирование путей:
      - входных файлов для обработки (source files)
      - конечного результата (destination path)
    """

    def __init__(self, tile: str, date: str, satellite: str):
        self.tile = tile
        self.date = date
        self.satellite = satellite

    @abstractmethod
    def get_sources(self, *, stage: str, agroid: Optional[int] = None) -> List[str]:
        """
        Возвращает список путей к файлам-источникам для данного этапа.
        Примеры stage: 'tile', 'index', 'sentinel_warp', ...
        """
        pass

    @abstractmethod
    def get_destination(self, *, stage: str, agroid: Optional[int] = None) -> str:
        """
        Возвращает путь, куда положить выходной файл для данного этапа.
        """
        pass
