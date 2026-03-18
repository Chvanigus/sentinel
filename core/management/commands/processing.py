"""Команда по полному циклу поиска и обработки изображений."""

from core.management.base import BaseCommand
from processing.orchestrator import SentinelProcessingOrchestrator


class Command(BaseCommand):
    help = "Команда полного цикла обработки спутниковых изображений."

    def add_arguments(self, parser):
        parser.add_argument(
            "-d", "--debug", action="store_true",
            help="Режим разработчика. Не удаляются отработанные файлы."
        )
        parser.add_argument(
            "--archive", action="store_true",
            help="Читать исходники из архива вместо DOWNLOADS_DIR"
        )
        parser.add_argument(
            "--archive-root", type=str,
            help="Корневая директория архива "
                 "(переопределяет settings.SNAPSHOTS_ARCHIVE_ROOT)"
        )
        parser.add_argument(
            "--start-year", type=int, default=2010,
            help="Год, с которого начинать обход архива (по умолчанию 2010)"
        )

    def handle(self, *args, **options):
        debug = options.get("debug", False)
        archive = options.get("archive", False)
        archive_root = options.get("archive_root")
        start_year = options.get("start_year", 2010)

        orchestrator = SentinelProcessingOrchestrator()
        if archive:
            orchestrator.run_from_archive(archive_root=archive_root,
                                          start_year=start_year, debug=debug)
        else:
            orchestrator.run(debug=debug)
