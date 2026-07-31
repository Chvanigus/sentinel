"""Команда для очистки данных при прерывании обработки спутниковых снимков."""
from core import settings
from core.filesystem import clear_directory_contents
from core.management.base import BaseCommand


class Command(BaseCommand):
    """Команда очистки промежуточных директорий после обработки снимков."""

    help = (
        "Команда для очистки данных при обработки спутниковых снимков. "
        "Очищает все директории кроме папки downloads, "
        "если не указан параметр -rdw (--rm_download)."
    )

    def add_arguments(self, parser):
        """Добавляет флаг очистки каталога загруженных архивов."""
        parser.add_argument(
            "-rdw", "--rm_download",
            help="Удалить данные из папки загрузки",
            action="store_true"
        )

    def handle(self, *args, **options):
        """Очищает рабочие каталоги с учётом флага загрузок."""
        rm_download = options.get("rm_download", False)

        remove_dirs = [
            settings.INTERMEDIATE,
            settings.PROCESSED_DIR,
            settings.NDVI_DIR,
            settings.TEMP_PROCESSING_DIR,
        ]

        if rm_download:
            remove_dirs.append(settings.DOWNLOADS_DIR)
            self.logger.info(
                "Опция --rm_download задана: удаляю также %s",
                settings.DOWNLOADS_DIR
            )
        else:
            self.logger.info(
                "Опция --rm_download не задана: не трогаю %s",
                settings.DOWNLOADS_DIR
            )

        self.logger.info(
            "Начинаю очистку директорий: %s",
            ", ".join(filter(None, remove_dirs)
                      )
        )
        clear_directory_contents(*remove_dirs)
        self.logger.info("Очистка завершена.")
