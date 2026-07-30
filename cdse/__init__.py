"""Модуль поиска и скачивания данных с CDSE."""
from .auth import CdseCredentials, CdseTokenProvider
from .client import CdseODataClient
from .download import ODataProductDownloader
from .exceptions import (
    CdseAuthError,
    CdseConfigError,
    CdseDownloadError,
    CdseError,
    CdseQueryError,
)
from .models import ProductRecord
from .orchestrator import SentinelDownloadOrchestrator
from .report import print_products_report
from .search import ODataProductSearcher
from .utils import build_archive_index

__all__ = [
    "CdseCredentials",
    "CdseTokenProvider",
    "CdseODataClient",
    "ODataProductDownloader",
    "SentinelDownloadOrchestrator",
    "ODataProductSearcher",
    "ProductRecord",
    "print_products_report",
    "build_archive_index",
    "CdseError",
    "CdseAuthError",
    "CdseQueryError",
    "CdseDownloadError",
    "CdseConfigError",
]
