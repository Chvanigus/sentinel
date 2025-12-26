"""Получение данных."""
from datetime import timedelta, datetime

import requests
from dateutil.parser import parse as parse_date

from core import settings
from core.management.base import BaseCommand, CommandError

PROXY_API = "https://sockslist.us/Api?request=display&country=all&level=all&token=free"


class Command(BaseCommand):
    help = (
        "Скачивание Sentinel-2 L2A снимков с "
        "dataspace.copernicus.eu по тайлу и дате."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-s", "--startdate", type=str, help="Дата начала (YYYY-MM-DD)"
        )
        parser.add_argument(
            "-e", "--enddate", type=str, help="Дата конца (YYYY-MM-DD)"
        )
        parser.add_argument(
            "-dw", "--download", action="store_true",
            help="Скачивание .SAFE"
        )

    def handle(self, *args, **options):
        # raise CommandError("Команда ещё не реализована")
        self.run(**options)

    def run(self, **options):
        return
