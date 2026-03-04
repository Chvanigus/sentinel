"""Класс логирования с поддержкой tqdm, ротации файлов и LoggerAdapter."""
from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from typing import Optional, Any, Dict

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    tqdm = None
    HAS_TQDM = False


class _TqdmHandler(logging.StreamHandler):
    """StreamHandler, совместимый с tqdm: использует tqdm.write для вывода."""

    def __init__(self, stream=None):
        # stream игнорируется, т.к. используем tqdm.write
        super().__init__(stream or sys.stdout)

    def emit(self, record: logging.LogRecord) -> None:
        """Выводит сообщение в tqdm."""
        try:
            msg = self.format(record)
            if HAS_TQDM:
                # tqdm.write сам выводит и корректно работает с прогресс-барами
                tqdm.write(msg, end="\n")
            else:
                stream = self.stream or sys.stdout
                stream.write(msg + self.terminator)
                self.flush()
        except Exception as e:
            self.handleError(record)


class SentinelLogger:
    """
    Кастомный логгер проекта SENTINEL.

    Параметры:
        name: имя логгера (logger name)
        log_file: путь к файлу логов (если None — файл не создаётся)
        level: минимальный уровень логирования (по умолчанию DEBUG)
        use_tqdm: пытаться ли использовать tqdm-aware хендлер
                  (по умолчанию True)
    """

    def __init__(
            self,
            name: str,
            log_file: Optional[str] = None,
            level: int = logging.DEBUG,
            use_tqdm: bool = True,
            rotate: Optional[Dict[str, int]] = None,
    ) -> None:
        self.logger = logging.getLogger(name)

        if getattr(self.logger, "_sentinel_initialized", False):
            self.logger.setLevel(level)
            self.adapter = logging.LoggerAdapter(self.logger, extra={})
            return

        self.logger.setLevel(level)

        # ===== Console handler =====
        if use_tqdm:
            ch = _TqdmHandler()
        else:
            ch = logging.StreamHandler(sys.stdout)

        ch.setLevel(level)
        ch.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        self.logger.addHandler(ch)

        # ===== File handler =====
        if log_file:
            folder = os.path.dirname(os.path.abspath(log_file))
            if folder and not os.path.exists(folder):
                os.makedirs(folder, exist_ok=True)

            if rotate and isinstance(rotate, dict):
                fh = logging.handlers.RotatingFileHandler(
                    log_file,
                    maxBytes=int(rotate.get("maxBytes", 10 * 1024 * 1024)),
                    backupCount=int(rotate.get("backupCount", 5)),
                    encoding="utf-8",
                )
            else:
                fh = logging.FileHandler(log_file, encoding="utf-8")

            fh.setLevel(level)
            fh.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            self.logger.addHandler(fh)

        self.logger.propagate = False
        setattr(self.logger, "_sentinel_initialized", True)
        self.adapter = logging.LoggerAdapter(self.logger, extra={})

    # Низкоуровневые прокси — сохраняют сигнатуру logging.Logger
    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Отладочное сообщение."""
        self.adapter.debug(message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Информационное сообщение."""
        self.adapter.info(message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Предупреждение."""
        self.adapter.warning(message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Ошибка."""
        self.adapter.error(message, *args, **kwargs)

    def critical(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Критическая ошибка."""
        self.adapter.critical(message, *args, **kwargs)

    def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Исключение."""
        self.adapter.exception(message, *args, **kwargs)

    # Удобства
    def set_level(self, level: int) -> None:
        """Меняет уровень логирования для логгера и всех хендлеров."""
        self.logger.setLevel(level)
        for h in self.logger.handlers:
            h.setLevel(level)

    def with_extra(self, **extra: Any) -> "SentinelLogger":
        """Возвращает новый LoggerAdapter с заданным extra.
        Пример: log.with_extra(request_id='abc').info('...')"""
        adapter = logging.LoggerAdapter(self.logger, extra=extra)
        new = SentinelLogger.__new__(SentinelLogger)
        # shallow copy существующего состояния
        new.logger = self.logger
        new.adapter = adapter
        return new

    def close(self) -> None:
        """Закрыть хендлеры (flush + close) — полезно в тестах/при остановке процесса."""
        for h in list(self.logger.handlers):
            try:
                h.flush()
                h.close()
            except Exception as e:
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.exception(
                        "Ошибка при закрытии хендлера %s: %s", h, e
                    )
            finally:
                self.logger.removeHandler(h)
        setattr(self.logger, "_sentinel_initialized", False)


def get_logger(name: str = __name__, **kwargs) -> SentinelLogger:
    """
    Фабрика: возвращает экземпляр SentinelLogger.

    kwargs поддерживает параметры конструктора SentinelLogger:
        log_file, level, use_tqdm, rotate, json
    """
    return SentinelLogger(name, **kwargs)
