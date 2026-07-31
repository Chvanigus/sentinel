"""Базовые классы команд."""
import argparse
import os
from abc import ABC, abstractmethod

from core.logging import get_logger


class BaseCommand(ABC):
    """Общий разбор аргументов и запуск консольной команды."""

    help: str = ""

    def __init__(self) -> None:
        """Создаёт logger конкретного класса команды."""
        self.logger = get_logger(name=self.__class__.__name__)

    def create_parser(
            self,
            prog_name: str,
            subcommand: str,
    ) -> argparse.ArgumentParser:
        """Создаёт parser аргументов конкретной подкоманды."""
        parser = argparse.ArgumentParser(
            prog=f"{os.path.basename(prog_name)} {subcommand}",
            description=self.help or None,
        )
        self.add_arguments(parser)
        return parser

    def run_from_argv(self, argv: list[str]) -> None:
        """Разбирает argv и напрямую вызывает обработчик команды."""
        parser = self.create_parser(argv[0], argv[1])
        options = parser.parse_args(argv[2:])
        cmd_options = vars(options)
        args = cmd_options.pop("args", ())
        self.handle(*args, **cmd_options)

    @abstractmethod
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Добавляет специфичные аргументы команды."""
        pass

    @abstractmethod
    def handle(self, *args, **options) -> None:
        """Выполняет сценарий команды с разобранными параметрами."""
        pass
