"""
Команда загрузки спутниковых снимков Sentinel-2
из Copernicus Data Space Ecosystem.
"""
import os
from datetime import date

from cdse_downloader.downloader import S3Downloader
from cdse_downloader.orchestrator import SentinelDownloadOrchestrator
from cdse_downloader.searching import DataSpaceSearcher
from cdse_downloader.utils import build_archive_index
from core.logging import get_logger
from core.management.base import BaseCommand
from core.settings import (TARGET_TILES,
                           L2A_COLLECTION,
                           L1C_COLLECTION)

logger = get_logger("CDSE Downloader")


class Command(BaseCommand):
    """
    Команда запуска процесса поиска
    и скачивания спутниковых снимков.
    """

    help = "Загрузка снимков Sentinel-2 из CDSE"

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

        key = os.environ.get("CDSE_S3_KEY")
        secret = os.environ.get('CDSE_S3_SECRET')
        s3_client = S3Downloader(
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            endpoint_url="https://eodata.dataspace.copernicus.eu"
        )

        orchestrator = SentinelDownloadOrchestrator(
            searcher=DataSpaceSearcher(),
            downloader=s3_client
        )

        archive_index = build_archive_index()

        products = orchestrator.search(
            L2A_COLLECTION, start, end, do_download, TARGET_TILES,
            archive_index=archive_index
        )
        if not products:
            logger.info("Нет L2A — пробуем L1C")
            products = orchestrator.search(
                L1C_COLLECTION, start, end, do_download, TARGET_TILES,
                archive_index=archive_index
            )

        if do_download:
            orchestrator.download(
                products, workers=1, archive_index=archive_index
            )

        logger.info("Завершено")
