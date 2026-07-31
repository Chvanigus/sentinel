"""
Команда загрузки спутниковых снимков Sentinel-2
из Copernicus Data Space Ecosystem (через OData).
"""
from datetime import date

from cdse.composition import build_cdse_service
from cdse.report import print_products_report
from cdse.utils import build_archive_index
from core.logging import get_logger
from core.management.base import BaseCommand
from core.settings import (
    ARCHIVE_ROOT,
    L2A_COLLECTION,
    L2A_PRODUCT_TYPE,
    TARGET_TILES,
)

logger = get_logger("CDSE Downloader")


class Command(BaseCommand):
    """
    Команда запуска процесса поиска
    и скачивания спутниковых снимков.
    """

    help = "Загрузка снимков Sentinel-2 из CDSE (OData)"

    def add_arguments(self, parser):
        """Добавляет диапазон поиска и флаг скачивания продуктов."""
        parser.add_argument("--start", required=True)
        parser.add_argument("--end")
        parser.add_argument(
            "--download",
            action="store_true",
            help="Скачивать найденные продукты"
        )

    def handle(self, *args, **options):
        """Ищет продукты, печатает отчёт и при необходимости скачивает архивы."""
        start = options["start"]
        end = options["end"] or date.today().isoformat()
        do_download = options.get("download")

        service = build_cdse_service()

        archive_index = build_archive_index(ARCHIVE_ROOT)

        products = service.search(
            collection=L2A_COLLECTION,
            start=start,
            end=end,
            do_download=do_download,
            tiles=TARGET_TILES,
            archive_index=archive_index,
            product_type=L2A_PRODUCT_TYPE,
        )

        all_records = []
        for items in products.values():
            all_records.extend(items)

        print_products_report(all_records, archive_base=ARCHIVE_ROOT)

        if do_download:
            service.download(
                products,
                workers=4,
                archive_root=ARCHIVE_ROOT,
            )

        logger.info("Завершено")
