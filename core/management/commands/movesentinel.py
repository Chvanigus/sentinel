"""Команда по перемещению снимков из рабочей директории в архив."""
import re
import shutil
from pathlib import Path

from core import settings
from core.logging import get_logger
from core.management.base import BaseCommand
from satgeo.client import GeoServerClient
from geoserver.catalog import FailedRequestError


class Command(BaseCommand):
    """Команда archive."""
    help = (
        "Переносит файлы Sentinel 2016-2018 из SENTINEL2024\n"
        "в папки SENTINEL2016, SENTINEL2017, SENTINEL2018 и обновляет их в GeoServer"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать действия без выполнения'
        )

    def handle(self, *args, **options):
        self.run(**options)

    def run(self, *args, **options):
        dry_run = options.get('dry_run', False)
        logger = get_logger()
        client = GeoServerClient()

        base_dir = Path('/mnt/map/geoware/SENTINEL2024')
        pattern = re.compile(
            r's2[abc]_(?P<day>\d{2})_(?P<month>\d{2})_(?P<year>\d{4})_.*\.tif$'
        )

        for file_path in base_dir.iterdir():
            if not file_path.is_file():
                continue
            match = pattern.match(file_path.name)
            if not match:
                logger.warning(
                    f"Не удалось распознать имя файла {file_path.name}, пропускаем")
                continue

            year = match.group('year')
            if year not in ('2016', '2017', '2018'):
                logger.debug(
                    f"Файл {file_path.name} за {year}, не подпадает под 2016-2018, пропускаем")
                continue

            dest_dir = Path(f'/mnt/map/geoware/SENTINEL{year}')
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / file_path.name

            logger.info(f"Перенос {file_path} -> {dest_path}")
            if not dry_run:
                shutil.move(str(file_path), str(dest_path))

            # Работа с GeoServer
            store_name = file_path.stem
            file_url = dest_path.as_posix()

            logger.info(
                f"Обновление GeoServer: store={store_name}, путь={file_url}"
            )
            if not dry_run:
                try:
                    store = client.catalog.get_store(store_name, client.workspace)
                except FailedRequestError:
                    self.logger.warning(
                        f"Не найден каталог для {store_name}. Пропуск"
                    )
                    continue
                if store:
                    store.url = f"file://{file_url}"
                    client.save_catalog(store)
                    logger.info(f"URL store '{store_name}' обновлён.")
                    client.seed_gwc(
                        store_name,
                        settings.ZOOM_START,
                        settings.ZOOM_STOP
                    )
                else:
                    logger.warning(f"Store '{store_name}' не найден, создаём новый.")
                    store = client.create_store(store_name, file_url)
                    client.create_coverage(store, store_name)

        logger.info("Перенос и обновление завершены.")
