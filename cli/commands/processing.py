"""Команда по полному циклу поиска и обработки изображений."""
from __future__ import annotations

import re
import unicodedata

from core.management.base import BaseCommand
from core.management.validators import resolve_date_range


def parse_agro_selector(value: str) -> tuple[int, ...]:
    """Разбирает список хозяйств вида ``3,4`` без повторов."""
    try:
        agroids = tuple(dict.fromkeys(
            int(item.strip()) for item in value.split(",") if item.strip()
        ))
    except ValueError as exc:
        raise ValueError("AGRO должен содержать числа через запятую") from exc
    if not agroids or any(agroid <= 0 for agroid in agroids):
        raise ValueError("AGRO должен содержать положительные числа")
    return agroids


def parse_field_selector(value: str) -> tuple[int, str]:
    """Разбирает Unicode-селектор поля вида ``A3/F100б``."""
    normalized = unicodedata.normalize("NFC", value.strip())
    match = re.fullmatch(
        r"A(?P<agroid>\d+)/(?P<fieldcode>F.+)",
        normalized,
        flags=re.IGNORECASE,
    )
    if match is None:
        raise ValueError("FIELD должен иметь формат A3/F100б")
    agroid = int(match.group("agroid"))
    if agroid <= 0:
        raise ValueError("Номер агро в FIELD должен быть положительным")
    return agroid, match.group("fieldcode")


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
        parser.add_argument(
            "--agro",
            type=parse_agro_selector,
            help="Пересчитать NDVI выбранных хозяйств, например 3,4.",
        )
        parser.add_argument(
            "--field",
            type=parse_field_selector,
            help=(
                "Пересчитать статистику NDVI поля по fieldcode, "
                "например A3/F100б."
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
        agro = options.get("agro")
        field = options.get("field")

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
        if (agro is not None or field is not None) and not recalculate_ndvi:
            raise ValueError(
                "--agro и --field доступны только с --recalculate-ndvi"
            )
        if agro is not None and field is not None:
            raise ValueError("Укажите либо --agro, либо --field")

        target_agroids = agro
        target_fieldcodes = None
        if field is not None:
            field_agroid, fieldcode = field
            target_agroids = (field_agroid,)
            target_fieldcodes = (fieldcode,)

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
            target_agroids=target_agroids,
            target_fieldcodes=target_fieldcodes,
        )
