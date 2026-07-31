"""Класс логирования с поддержкой tqdm, ротации файлов и LoggerAdapter."""
from __future__ import annotations

import logging
import logging.handlers
import os
import sys

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
        except Exception:
            self.handleError(record)


def get_logger(
        name: str = __name__,
        *,
        log_file: str | None = None,
        level: int = logging.DEBUG,
        use_tqdm: bool = True,
        rotate: dict[str, int] | None = None,
) -> logging.Logger:
    """Возвращает один настроенный стандартный logger для заданного имени."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if getattr(logger, "_sentinel_initialized", False):
        for handler in logger.handlers:
            handler.setLevel(level)
        return logger

    console = (
        _TqdmHandler()
        if use_tqdm
        else logging.StreamHandler(sys.stdout)
    )
    console.setLevel(level)
    console.setFormatter(
        logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    logger.addHandler(console)

    if log_file:
        folder = os.path.dirname(os.path.abspath(log_file))
        if folder:
            os.makedirs(folder, exist_ok=True)
        if rotate:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=int(rotate.get("maxBytes", 10 * 1024 * 1024)),
                backupCount=int(rotate.get("backupCount", 5)),
                encoding="utf-8",
            )
        else:
            file_handler = logging.FileHandler(
                log_file,
                encoding="utf-8",
            )
        file_handler.setLevel(level)
        file_handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(file_handler)

    logger.propagate = False
    logger._sentinel_initialized = True
    return logger
