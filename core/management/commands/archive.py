"""Команда по перемещению снимков из рабочей директории в архив."""
from datetime import datetime

from core.management.base import BaseCommand, CommandError
from core.management.validators import valid_date
from satgeo.archive import execute_mover


class Command(BaseCommand):
    """Команда archive."""
    help = ("Команда по перемещению спутниковых "
            "снимков из рабочей директории в архив.")

    def add_arguments(self, parser):
        parser.add_argument(
            "-s", "--startdate", type=valid_date,
            help="Начало диапазона дат, между которым "
                 "требуется перекинуть снимки",
        )
        parser.add_argument(
            "-e", "--enddate", type=valid_date,
            help="Конец диапазона дат, между которым требуется"
                 "перекинуть снимки"
        )
        parser.add_argument(
            "-y", "--year", type=int,
            help="Год, за который требуется перенести снимки"
        )

    def handle(self, *args, **options):
        start = options.get("startdate")
        end = options.get("enddate")
        year = options.get("year")

        if year:
            options.pop("startdate", None)
            options.pop("enddate", None)
        elif start and end:
            options.pop("year", None)
        else:
            raise CommandError(
                "Нужно указать либо параметр --year, "
                "либо оба параметра --startdate и --enddate."
            )

        if not year:
            options["year"] = datetime.now().year

        self.run(**options)

    def run(self, **options):
        execute_mover(**options)
