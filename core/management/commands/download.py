"""
Команда загрузки спутниковых снимков Sentinel-2
из Copernicus Data Space Ecosystem (через OData).
"""
import os
from datetime import date

from cdse import (
    CdseCredentials,
    CdseTokenProvider,
    CdseODataClient,
    ODataProductSearcher,
    ODataProductDownloader,
    SentinelDownloadOrchestrator,
    build_archive_index,
    print_products_report,
)
from core.logging import get_logger
from core.management.base import BaseCommand
from core.settings import (
    TARGET_TILES,
    L2A_COLLECTION,
    L1C_COLLECTION,
)

logger = get_logger("CDSE Downloader")


class Command(BaseCommand):
    """
    Команда запуска процесса поиска
    и скачивания спутниковых снимков.
    """

    help = "Загрузка снимков Sentinel-2 из CDSE (OData)"

    def add_arguments(self, parser):
        parser.add_argument("--start", required=True)
        parser.add_argument("--end")
        parser.add_argument(
            "--download",
            action="store_true",
            help="Скачивать найденные продукты"
        )

    def handle(self, *args, **options):
        start = options["start"]
        end = options["end"] or date.today().isoformat()
        do_download = options.get("download")

        username = os.environ.get("CDSE_USERNAME")
        password = os.environ.get("CDSE_PASSWORD")

        if not username or not password:
            raise RuntimeError(
                "Не заданы CDSE_USERNAME / CDSE_PASSWORD"
            )

        credentials = CdseCredentials(
            username=username,
            password=password,
            client_id="cdse-public",
            totp=os.environ.get("CDSE_TOTP"),
        )

        token_provider = CdseTokenProvider(credentials)
        client = CdseODataClient(token_provider)

        searcher = ODataProductSearcher(client)
        downloader = ODataProductDownloader(client)

        orchestrator = SentinelDownloadOrchestrator(
            searcher=searcher,
            downloader=downloader,
        )

        archive_index = build_archive_index()

        products = orchestrator.search(
            collection=L2A_COLLECTION,
            start=start,
            end=end,
            do_download=do_download,
            tiles=TARGET_TILES,
            archive_index=archive_index,
            product_type="S2MSI2A",
        )

        if not products:
            logger.info("Нет L2A — пробуем L1C")
            products = orchestrator.search(
                collection=L1C_COLLECTION,
                start=start,
                end=end,
                do_download=do_download,
                tiles=TARGET_TILES,
                archive_index=archive_index,
                product_type="S2MSI1C",
            )

        all_records = []
        for items in products.values():
            all_records.extend(items)

        print_products_report(all_records)

        if do_download:
            orchestrator.download(
                products,
                workers=4,
            )

        logger.info("Завершено")
