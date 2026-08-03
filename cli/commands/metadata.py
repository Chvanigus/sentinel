"""Команда обновления метаданных уже опубликованных спутниковых слоёв."""
from core.management.base import BaseCommand
from core.management.validators import resolve_date_range


class Command(BaseCommand):
    """Обновляет ``maps_layer`` без обработки растров и публикации GeoServer."""

    help = (
        "Перерасчёт метаданных существующих снимков из локальных ZIP и "
        "статистики NDVI."
    )

    def add_arguments(self, parser):
        """Добавляет необязательный календарный диапазон обновления."""
        parser.add_argument("--year", type=int)
        parser.add_argument("--month", type=int)
        parser.add_argument("--start")
        parser.add_argument("--end")

    def handle(self, *args, **options):
        """Нормализует период и запускает быстрый metadata-сценарий."""
        start_date, end_date = resolve_date_range(
            year=options.get("year"),
            month=options.get("month"),
            start=options.get("start"),
            end=options.get("end"),
        )

        from processing.composition import refresh_layer_metadata

        refresh_layer_metadata(
            start_date=start_date,
            end_date=end_date,
        )
