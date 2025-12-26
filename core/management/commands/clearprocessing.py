"""Команда для очистки данных при прерывании обработки спутниковых снимков."""
from core import settings
from core.management.base import BaseCommand
from core.utils import remove_files_from_dir


class Command(BaseCommand):
    """Команда processing."""
    help = (
        "Команда для очистки данных при обработки спутниковых снимков. "
        "Очищает все директории кроме папки downloads, "
        "если не указан параметр -rdw (--remove_download)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-rdw", "--rm_download",
            help="Удаление данных из папки загрузки",
            action="store_true"
        )

    def handle(self, *args, **options):
        self.run(**options)

    def run(self, **options):
        remove_dirs = [
            settings.INTERMEDIATE,
            settings.PROCESSED_DIR,
            settings.COMBINE_DIR,
            settings.NDVI_DIR,
            settings.TEMP_PROCESSING_DIR,
        ]

        if options["rm_download"]:
            remove_dirs.append(settings.DOWNLOADS_DIR)

        remove_files_from_dir(*remove_dirs)
