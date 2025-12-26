"""Класс логирования."""
import logging
import sys


class SentinelLogger:
    """Кастомный логгер проекта SENTINEL с поддержкой tqdm-detect."""
    def __init__(self, name: str, log_file: str = None) -> None:
        self.logger = logging.getLogger(name)

        if not self.logger.handlers:
            self.logger.setLevel(logging.DEBUG)

            # Хендлер для вывода в консоль
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S"))
            self.logger.addHandler(ch)

            # Хендлер для вывода в файл (если указан)
            if log_file:
                fh = logging.FileHandler(log_file)
                fh.setLevel(logging.DEBUG)
                fh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
                self.logger.addHandler(fh)

            # Отключаем проксирование в корневой логгер
            self.logger.propagate = False

    def info(self, message: str) -> None:
        """Уровень INFO."""
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Уровень WARNING."""
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Уровень ERROR."""
        self.logger.error(message)

    def critical(self, message: str) -> None:
        """Уровень CRITICAL."""
        self.logger.critical(message)

    def debug(self, message: str) -> None:
        """Уровень DEBUG."""
        self.logger.debug(message)


def get_logger() -> SentinelLogger:
    """Возвращает логгер."""
    return SentinelLogger("sentinel")
