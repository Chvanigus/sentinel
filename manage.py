"""Основной управляющий файл проектом sentinel."""
import sys


def main() -> int:
    """Запуск проекта с помощью команд."""
    try:
        from core.management.manager import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Невозможно импортировать модуль вызова команд проект SENTINEL"
        ) from exc

    return execute_from_command_line(sys.argv)


if __name__ == '__main__':
    raise SystemExit(main())
