"""Базовый lifecycle процессоров и общий контекст path resolvers."""
from __future__ import annotations

from abc import ABC, abstractmethod

from core.logging import get_logger
from processing.domain import SceneContext


class BaseImageProcessor(ABC):
    """Общий контекст и logger GIS-процессоров."""

    def __init__(
            self,
            scene: SceneContext,
            paths,
    ):
        self.scene = scene
        self.paths = paths
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def run(self) -> None:
        """Выполняет специализированную обработку."""


__all__ = [
    "BaseImageProcessor",
]
