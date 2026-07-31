"""Типизированный pipeline обработки одной сцены."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from core.logging import get_logger

from .domain import SceneContext
from .exceptions import ProcessingStepError

SceneStepHandler = Callable[[SceneContext], Any]


@dataclass(frozen=True)
class SceneStep:
    """Именованный шаг обработки сцены."""

    name: str
    handler: SceneStepHandler


class ScenePipeline:
    """Последовательно выполняет шаги, не зная об их инфраструктуре."""

    def __init__(self, steps: Sequence[SceneStep]):
        if not steps:
            raise ValueError("Pipeline должен содержать хотя бы один шаг")
        self._steps = tuple(steps)
        self.logger = get_logger(self.__class__.__name__)

    @property
    def step_names(self) -> tuple[str, ...]:
        """Возвращает имена шагов в порядке исполнения."""
        return tuple(step.name for step in self._steps)

    def run(self, scene: SceneContext) -> None:
        """Выполняет шаги и оборачивает ошибку именем текущего этапа."""
        pipeline_started = perf_counter()
        context = (
            f"{scene.satellite}/{scene.tile}/"
            f"{scene.acquired_on.isoformat()}/{scene.level.value}"
        )
        self.logger.info(
            "PIPELINE START: %s | шагов=%d",
            context,
            len(self._steps),
        )
        for step in self._steps:
            step_started = perf_counter()
            self.logger.info("STEP START: %s | %s", step.name, context)
            try:
                result = step.handler(scene)
            except Exception as exc:
                self.logger.exception(
                    "STEP FAIL: %s | %s | %.2f сек. | %s",
                    step.name,
                    context,
                    perf_counter() - step_started,
                    exc,
                )
                raise ProcessingStepError(step.name, str(exc)) from exc

            if result is False:
                reason = "обработчик вернул False"
                self.logger.error(
                    "STEP FAIL: %s | %s | %.2f сек. | %s",
                    step.name,
                    context,
                    perf_counter() - step_started,
                    reason,
                )
                raise ProcessingStepError(step.name, reason)

            self.logger.info(
                "STEP OK: %s | %s | %.2f сек.",
                step.name,
                context,
                perf_counter() - step_started,
            )
        self.logger.info(
            "PIPELINE OK: %s | %.2f сек.",
            context,
            perf_counter() - pipeline_started,
        )
