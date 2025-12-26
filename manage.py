"""Основной управляющий файл проектом sentinel."""
import sys


def main() -> None:
    """Запуск проекта с помощью команд."""
    try:
        from core.management.manager import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Невозможно импортировать модуль вызова команд проект SENTINEL"
        ) from exc

    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
