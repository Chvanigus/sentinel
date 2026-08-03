"""Команда по полному циклу поиска и обработки изображений."""
from __future__ import annotations

from core.management.base import BaseCommand
from core.management.validators import resolve_date_range


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
