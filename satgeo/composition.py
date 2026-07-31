"""Сборка production-зависимостей публикации."""

from core import settings

from .client import GeoServerClient, GeoServerConfig
from .publisher import (
    PostgisPublicationRepository,
    PublicationPlanner,
    RasterPublisher,
)


def build_raster_publisher() -> RasterPublisher:
    """Собирает production-сервис публикации из настроек окружения."""
    return RasterPublisher(
        source_root=settings.PROCESSED_DIR,
        workspace=settings.GS_WORKSPACE,
        current_year=settings.YEAR,
        planner=PublicationPlanner(
            settings.GS_DATA_ROOT,
            settings.GS_DATA_DIR,
        ),
        client=GeoServerClient(
            GeoServerConfig(
                host=settings.GS_HOST,
                workspace=settings.GS_WORKSPACE,
                username=settings.GS_USERNAME,
                password=settings.GS_PASSWORD,
            )
        ),
        repository=PostgisPublicationRepository(),
    )
