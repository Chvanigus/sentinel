"""Менеджер запуска команд."""
import os
import sys
from importlib import import_module

from core.logging import get_logger
from core.management.base import BaseCommand


def load_command_class(name: str) -> BaseCommand:
    """
    Возвращает класс экземпляра (класс Command) команды по её названию.
    :param name: Название команды
    :return: Класс выбранной команды.
    """
    module = import_module(f"core.management.commands.{name}")
    return module.Command()


class ManagementUtility:
    """Утилита менеджера консольных команд."""

    def __init__(self, argv=None):
        self.argv = argv or sys.argv[:]
        self.prog_name = os.path.basename(self.argv[0])
        self.logger = get_logger()

    def fetch_command(self, subcommand) -> BaseCommand:
        """Возвращает класс команды."""
        # try:
        return load_command_class(subcommand)
        # except ModuleNotFoundError:
        #     self.logger.info(
        #         f"Введите '{self.prog_name} help' "
        #         f"для просмотра инструкций по использованию"
        #     )

    def execute(self):
        """Выполняет команду."""
        try:
            subcommand = self.argv[1]
        except IndexError:
            subcommand = "help"

        if subcommand == 'help':
            # @TODO доделать инструкцию к manage.py
            self.logger.info("Инструкция ещё пишется.")

        else:
            module = self.fetch_command(subcommand)

            if module is not None:
                module.run_from_argv(self.argv)


def execute_from_command_line(argv=None):
    """Запуск ManagementUtility."""
    utility = ManagementUtility(argv)
    utility.execute()
