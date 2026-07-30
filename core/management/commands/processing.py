"""Команда по полному циклу поиска и обработки изображений."""
from datetime import datetime

from core.management.base import BaseCommand
from processing.orchestrator import SentinelProcessingOrchestrator


class Command(BaseCommand):
    help = "Команда полного цикла обработки спутниковых изображений."

    def add_arguments(self, parser):
        parser.add_argument(
            "-d", "--debug", action="store_true",
            help="Режим разработчика. Не удаляются отработанные файлы."
        )

        parser.add_argument("--year", type=int)
        parser.add_argument("--month", type=int)

        parser.add_argument("--start")
        parser.add_argument("--end")

    def handle(self, *args, **options):
        debug = options.get("debug", False)

        year = options.get("year")
        month = options.get("month")
        start = options.get("start")
        end = options.get("end")

        start_date = None
        end_date = None

        if start:
            start_date = datetime.strptime(start, "%Y-%m-%d")

        if end:
            end_date = datetime.strptime(end, "%Y-%m-%d")

        if year and not start_date:
            if month:
                start_date = datetime(year, month, 1)

                if month == 12:
                    end_date = datetime(year + 1, 1, 1)
                else:
                    end_date = datetime(year, month + 1, 1)
            else:
                start_date = datetime(year, 1, 1)
                end_date = datetime(year + 1, 1, 1)

        orchestrator = SentinelProcessingOrchestrator()

        orchestrator.run(
            debug=debug,
            start_date=start_date,
            end_date=end_date,
        )
