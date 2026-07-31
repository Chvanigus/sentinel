"""Команда по полному циклу поиска и обработки изображений."""
from __future__ import annotations

from datetime import datetime, timedelta

from core.management.base import BaseCommand


def resolve_date_range(
        *,
        year: int | None = None,
        month: int | None = None,
        start: str | None = None,
        end: str | None = None,
) -> tuple[datetime | None, datetime | None]:
    """Возвращает полуоткрытый диапазон дат ``[start, end)``."""
    if month is not None and year is None:
        raise ValueError("--month можно использовать только вместе с --year")
    if (year is not None or month is not None) and (start or end):
        raise ValueError(
            "--year/--month нельзя смешивать с --start/--end"
        )

    if start or end:
        start_date = (
            datetime.strptime(start, "%Y-%m-%d") if start else None
        )
        # Пользовательская конечная дата включительна; внутри используем [start, end).
        end_date = (
            datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
            if end
            else None
        )
    elif year is not None:
        start_date = datetime(year, month or 1, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        elif month is not None:
            end_date = datetime(year, month + 1, 1)
        else:
            end_date = datetime(year + 1, 1, 1)
    else:
        start_date = end_date = None

    if start_date and end_date and start_date >= end_date:
        raise ValueError("Дата начала должна быть не позже даты окончания")
    return start_date, end_date


class Command(BaseCommand):
    """Запускает полный сценарий обработки за выбранный период."""

    help = "Команда полного цикла обработки спутниковых изображений."

    def add_arguments(self, parser):
        """Добавляет режим отладки и взаимоисключающие варианты периода."""
        parser.add_argument(
            "-d", "--debug", action="store_true",
            help="Режим разработчика. Не удаляются отработанные файлы."
        )
        parser.add_argument(
            "--recalculate-ndvi",
            action="store_true",
            help=(
                "Полностью пересчитать NDVI из локальных ZIP-архивов, "
                "заменив растры, статистику БД и кэш публикации."
            ),
        )

        parser.add_argument("--year", type=int)
        parser.add_argument("--month", type=int)

        parser.add_argument("--start")
        parser.add_argument("--end")

    def handle(self, *args, **options):
        """Преобразует аргументы в диапазон дат и запускает application service."""
        debug = options.get("debug", False)
        recalculate_ndvi = options.get("recalculate_ndvi", False)

        year = options.get("year")
        month = options.get("month")
        start = options.get("start")
        end = options.get("end")
        if recalculate_ndvi and debug:
            raise ValueError(
                "--recalculate-ndvi нельзя использовать вместе с --debug"
            )
        if recalculate_ndvi and year is None and start is None:
            raise ValueError(
                "Для полного перерасчёта укажите --year либо --start"
            )

        start_date, end_date = resolve_date_range(
            year=year,
            month=month,
            start=start,
            end=end,
        )

        from processing.composition import build_processing_service

        build_processing_service(
            recalculate_ndvi=recalculate_ndvi,
        ).run(
            debug=debug,
            start_date=start_date,
            end_date=end_date,
        )
