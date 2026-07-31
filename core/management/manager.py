"""Менеджер запуска команд."""
import os
import pkgutil
import sys
from importlib import import_module

from cli import commands
from core.logging import get_logger
from core.management.base import BaseCommand


def get_command_names() -> list[str]:
    """Возвращает доступные команды в стабильном порядке."""
    return sorted(
        module.name
        for module in pkgutil.iter_modules(commands.__path__)
        if not module.name.startswith("_")
    )


def load_command_class(name: str) -> BaseCommand:
    """
    Возвращает класс экземпляра (класс Command) команды по её названию.
    :param name: Название команды
    :return: Класс выбранной команды.
    """
    module = import_module(f"cli.commands.{name}")
    return module.Command()


class ManagementUtility:
    """Утилита менеджера консольных команд."""

    def __init__(self, argv=None):
        self.argv = argv or sys.argv[:]
        self.prog_name = os.path.basename(self.argv[0])
        self.logger = get_logger()

    def fetch_command(self, subcommand) -> BaseCommand:
        """Возвращает класс команды."""
        command_names = get_command_names()
        if subcommand not in command_names:
            available = ", ".join(command_names)
            raise ValueError(
                f"Неизвестная команда '{subcommand}'. "
                f"Доступные команды: {available}"
            )
        return load_command_class(subcommand)

    def execute(self) -> int:
        """Выполняет команду."""
        try:
            subcommand = self.argv[1]
        except IndexError:
            subcommand = "help"

        if subcommand == "help":
            self.logger.info(
                "Использование: %s <команда> [параметры]",
                self.prog_name,
            )
            self.logger.info(
                "Доступные команды: %s", ", ".join(get_command_names())
            )
            return 0

        try:
            command = self.fetch_command(subcommand)
        except ValueError as exc:
            self.logger.error("%s", exc)
            return 2

        command.run_from_argv(self.argv)
        return 0


def execute_from_command_line(argv=None) -> int:
    """Запуск ManagementUtility."""
    utility = ManagementUtility(argv)
    return utility.execute()
