"""Тесты настроек и инфраструктуры консольных команд."""

from datetime import datetime
from pathlib import Path

import pytest

from cli.commands.processing import Command as ProcessingCommand
from cli.commands.processing import resolve_date_range
from core import settings
from core.management.base import BaseCommand
from core.management.manager import ManagementUtility, get_command_names


class RecordingCommand(BaseCommand):
    """Минимальная команда для проверки разбора и передачи параметров."""

    def __init__(self):
        """Создаёт команду без выполненных вызовов."""
        super().__init__()
        self.calls = []

    def add_arguments(self, parser):
        """Добавляет обязательный числовой параметр."""
        parser.add_argument("--count", required=True, type=int)

    def handle(self, *args, **options):
        """Запоминает разобранные позиционные и именованные параметры."""
        self.calls.append((args, options))


def test_archive_directory_uses_single_root_and_normalized_tile():
    """Путь архива использует единый корень и нормализованный тайл."""
    archive_dir = Path(settings.get_archive_dir("2026", "T38ula"))

    assert archive_dir.parent == Path(settings.ARCHIVE_ROOT) / "2026"
    assert archive_dir.name == "38ULA"


def test_current_year_is_integer():
    """Текущий год в настройках хранится числом."""
    assert settings.YEAR == datetime.now().year


def test_processing_month_uses_half_open_range():
    """Месячный период представлен полуоткрытым диапазоном."""
    assert resolve_date_range(year=2026, month=7) == (
        datetime(2026, 7, 1),
        datetime(2026, 8, 1),
    )


def test_processing_explicit_end_date_is_inclusive():
    """Явно заданная конечная дата включается в пользовательский период."""
    assert resolve_date_range(
        start="2026-07-01",
        end="2026-07-31",
    ) == (
        datetime(2026, 7, 1),
        datetime(2026, 8, 1),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"month": 7},
        {"year": 2026, "start": "2026-07-01"},
        {"start": "2026-07-02", "end": "2026-07-01"},
    ],
)
def test_processing_rejects_ambiguous_or_invalid_range(kwargs):
    """Противоречивые и обратные диапазоны дат отклоняются."""
    with pytest.raises(ValueError):
        resolve_date_range(**kwargs)


def test_ndvi_recalculation_requires_explicit_period(monkeypatch):
    """Разрушающий перерасчёт нельзя случайно запустить на весь архив."""
    command = ProcessingCommand()

    with pytest.raises(ValueError, match="укажите --year либо --start"):
        command.handle(
            debug=False,
            recalculate_ndvi=True,
            year=None,
            month=None,
            start=None,
            end=None,
        )


def test_management_help_discovers_commands():
    """Менеджер обнаруживает только поддерживаемые команды."""
    assert get_command_names() == ["clearprocessing", "download", "processing"]


def test_unknown_management_command_returns_nonzero():
    """Неизвестная команда завершается ненулевым кодом."""
    utility = ManagementUtility(["manage.py", "unknown"])

    assert utility.execute() == 2


def test_command_passes_parsed_options_directly_to_handle():
    """Базовая команда не добавляет промежуточных execute/run-прослоек."""
    command = RecordingCommand()

    command.run_from_argv(
        ["manage.py", "recording", "--count", "3"]
    )

    assert command.calls == [((), {"count": 3})]


def test_management_without_subcommand_prints_help_successfully():
    """Запуск без подкоманды показывает общую справку с успешным кодом."""
    assert ManagementUtility(["manage.py"]).execute() == 0
