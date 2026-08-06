"""
Команда загрузки спутниковых снимков Sentinel-2
из Copernicus Data Space Ecosystem (через OData).
"""
from datetime import date, timedelta

from cdse.composition import build_cdse_service
from cdse.report import print_products_report
from cdse.utils import build_archive_index
from core.logging import get_logger
from core.management.base import BaseCommand
from core.settings import (
    ARCHIVE_ROOT,
    DOWNLOAD_WORKERS,
    L2A_COLLECTION,
    L2A_PRODUCT_TYPE,
    TARGET_TILES,
)

logger = get_logger("CDSE Downloader")

DEFAULT_LOOKBACK_DAYS = 3


def resolve_download_range(
        *,
        start: str | None,
        end: str | None,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        today: date | None = None,
) -> tuple[str, str]:
    """Возвращает включительный диапазон поиска для ручного или ночного запуска."""
    if lookback_days < 1:
        raise ValueError("--lookback-days должен быть положительным числом")

    end_date = date.fromisoformat(end) if end else (today or date.today())
    start_date = (
        date.fromisoformat(start)
        if start
        else end_date - timedelta(days=lookback_days - 1)
    )
    if start_date > end_date:
        raise ValueError("Дата начала не может быть позже даты окончания")
    return start_date.isoformat(), end_date.isoformat()


class Command(BaseCommand):
    """
    Команда запуска процесса поиска
    и скачивания спутниковых снимков.
    """

    help = "Загрузка снимков Sentinel-2 из CDSE (OData)"

    def add_arguments(self, parser):
        """Добавляет диапазон поиска и флаг скачивания продуктов."""
        parser.add_argument("--start")
        parser.add_argument("--end")
        parser.add_argument(
            "--lookback-days",
            type=int,
            default=DEFAULT_LOOKBACK_DAYS,
            help=(
                "Количество календарных дней поиска, включая конечную дату, "
                f"если --start не задан (по умолчанию: {DEFAULT_LOOKBACK_DAYS})"
            ),
        )
        parser.add_argument(
            "--download",
            action="store_true",
            help="Скачивать найденные продукты"
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=DOWNLOAD_WORKERS,
            help=(
                "Количество одновременных загрузок "
                f"(по умолчанию: {DOWNLOAD_WORKERS})"
            ),
        )

    def handle(self, *args, **options):
        """Ищет продукты, печатает отчёт и при необходимости скачивает архивы."""
        start, end = resolve_download_range(
            start=options["start"],
            end=options["end"],
            lookback_days=options["lookback_days"],
        )
        do_download = options.get("download")

        service = build_cdse_service()

        archive_index = build_archive_index(
            ARCHIVE_ROOT,
            start_date=date.fromisoformat(start),
            end_date=date.fromisoformat(end),
            tiles=tuple(TARGET_TILES),
        )

        products = service.search(
            collection=L2A_COLLECTION,
            start=start,
            end=end,
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
                workers=options["workers"],
                archive_root=ARCHIVE_ROOT,
            )

        logger.info("Завершено")
