"""Команда для очистки данных при прерывании обработки спутниковых снимков."""
from core import settings
from core.management.base import BaseCommand
from core.utils import remove_files_from_dir


class Command(BaseCommand):
    """Команда очистки промежуточных директорий после обработки снимков."""

    help = (
        "Команда для очистки данных при обработки спутниковых снимков. "
        "Очищает все директории кроме папки downloads, "
        "если не указан параметр -rdw (--rm_download)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-rdw", "--rm_download",
            help="Удалить данные из папки загрузки",
            action="store_true"
        )

    def handle(self, *args, **options):
        """
        Точка входа команды: сразу выполняем очистку.
        Не вызываем self.run(), чтобы не зависеть от поведения базового run.
        """
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
        remove_files_from_dir(*remove_dirs)
        self.logger.info("Очистка завершена.")
