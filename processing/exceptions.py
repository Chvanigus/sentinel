"""Исключения application-слоя обработки."""
from __future__ import annotations

from collections.abc import Iterable


class ProcessingError(RuntimeError):
    """Базовая ошибка обработки."""


class ProcessingStepError(ProcessingError):
    """Ошибка конкретного шага обработки сцены."""

    def __init__(self, step: str, reason: str):
        self.step = step
        self.reason = reason
        super().__init__(f"{step}: {reason}")


class ProcessingRunError(ProcessingError):
    """Составная ошибка обработки нескольких дат."""

    def __init__(self, failed_dates: Iterable[str]):
        self.failed_dates = tuple(failed_dates)
        super().__init__(
            "Обработка завершилась с ошибками для дат: "
            + ", ".join(self.failed_dates)
        )
