"""Базовые классы команд."""
import argparse
import os
from abc import ABC, abstractmethod
from core.logging import get_logger


class CommandError(Exception):
    """
    Исключение, используемое для обработки ошибок в командных парсерах.
    """

    def __init__(self, *args, returncode=1):
        self.returncode = returncode
        super().__init__(*args)


class CommandParser(argparse.ArgumentParser):
    """
    Кастомный ArgumentParser с настраиваемыми параметрами для улучшения
    некоторых сообщений об ошибках и предотвращения вызова SystemExit.
    """

    def __init__(
            self, *, missing_args_message=None, called_from_command_line=None,
            **kwargs
    ):
        """
        Инициализация CommandParser.
        :param missing_args_message: Сообщение об ошибке, которое будет
                                     выведено, если не переданы обязательные
                                     аргументы.
        :param called_from_command_line: Флаг, указывающий, вызывается ли
                                         парсер аргументов из командной строки.
        :param **kwargs: Дополнительные аргументы, передаваемые в конструктор
                         класса ArgumentParser.
        """
        self.missing_args_message = missing_args_message
        self.called_from_command_line = called_from_command_line
        super().__init__(**kwargs)

    def parse_args(self, args=None, namespace=None) -> argparse.Namespace:
        """
        Парсит аргументы командной строки и возвращает объект пространства
        имен, содержащий значения аргументов.

        :param args: Список строк аргументов командной строки.
                Если не указан, будет использоваться sys.argv[1:].
        :param namespace: Пространство имен, в которое будут сохранены значения
                          аргументов. Если не указано, будет создано новое
                          пространство имен.

        :return: Пространство имен со значениями аргументов.

        :raises argparse.ArgumentTypeError: Если аргументы не могут быть
                                            распарсены.
        """
        if (self.missing_args_message and
                not (args or any(not arg.startswith("-") for arg in args))):
            self.error(self.missing_args_message)
        return super().parse_args(args, namespace)

    def error(self, message: str):
        """
        Выводит сообщение об ошибке и завершает выполнение программы с кодом
        ошибки, если парсер аргументов вызывается из командной строки, или
        вызывает CommandError.
        :param message: Сообщение об ошибке.
        """
        if self.called_from_command_line:
            super().error(message)
        else:
            raise CommandError(f"Ошибка: {message}")


class BaseCommand(ABC):
    """
    Абстрактный базовый класс команд.

    Этот класс предоставляет базовую функциональность для создания и
    выполнения команд в приложении SENTINEL.
    Класс предоставляет методы для создания парсера аргументов,
    вывода справочной информации, выполнения команды и обработки аргументов
    командной строки.

    Attributes:
        help: Справочное сообщение для команды. По умолчанию пустая строка.
        _called_from_command_line: Флаг, что команда была вызвана
                                   из командной строки
    """

    help: str = ""
    _called_from_command_line: bool = False

    def __init__(self):
        self.logger = get_logger()

    def create_parser(self, prog_name, subcommand, **kwargs) -> CommandParser:
        """
        Создает и возвращает кастомный CommandParser для команды.

        :param prog_name: Имя программы (manage.py).
        :param subcommand: Имя подкоманды (запускаемая команда).

        :return: Кастомный парсер аргументов.
        """
        parser = CommandParser(
            prog=f"{os.path.basename(prog_name)} {subcommand}",
            description=self.help or None,
            missing_args_message=getattr(self, "missing_args_message", None),
            called_from_command_line=getattr(
                self, "_called_from_command_line", None
            ),
            **kwargs
        )
        self.add_arguments(parser)
        return parser

    def print_help(self, *args, **options) -> None:
        """
        Выводит на печать справочное сообщение для этой команды.
        """
        parser = self.create_parser(*args, **options)
        parser.print_help()

    def execute(self, *args, **options):
        """
        Выполняет команду с переданными аргументами.
        """
        output = self.handle(*args, **options)

        if output:
            return output

    def run_from_argv(self, argv) -> None:
        """
        Запускает команду из командной строки.
        :param argv: Список аргументов командной строки.
        """
        parser = self.create_parser(argv[0], argv[1])
        options = parser.parse_args(argv[2:])

        cmd_options = vars(options)
        args = cmd_options.pop('args', ())

        self.execute(*args, **cmd_options)

    @abstractmethod
    def add_arguments(self, parser: CommandParser):
        """
        Добавляет аргументы для команды.
        """
        pass

    @abstractmethod
    def handle(self, *args, **options):
        """
        Обрабатывает команду с переданными аргументами.
        """
        pass

    @abstractmethod
    def run(self, **options):
        """
        Запуск действий.
        """
        pass
