"""Базовые классы модуля processing."""
from abc import ABC, abstractmethod
from typing import List, Optional

from core.logging import get_logger


class BaseImageProcessor(ABC):
    """Базовый класс обработки изображений."""

    def __init__(self,
                 tile: str,
                 date: str,
                 satellite: str,
                 path_manager,
                 level: str = None):
        self.tile = tile
        self.date = date
        self.satellite = satellite
        self.pm = path_manager
        self.level = level
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def _process_files(self, *args, **kwargs):
        """Основной метод класса обработки изображений."""
        pass

    def execute(self, *args, **kwargs):
        """
        Метод вызова класса обработки изображений.
        Обращение к методу обработки класса происходит через execute.
        """
        self._process_files(*args, **kwargs)


class BasePathManager(ABC):
    """
    Базовый класс, который отвечает за формирование путей:
      - входных файлов для обработки (source files)
      - конечного результата (destination path)
    """

    def __init__(self,
                 tile: str,
                 date: str,
                 satellite: str,
                 level: str = None):
        self.tile = tile
        self.date = date
        self.satellite = satellite
        self.level = level

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
