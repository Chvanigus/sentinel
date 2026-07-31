"""Сборка production-зависимостей CDSE."""
from __future__ import annotations

import os

import requests

from core.settings import (
    API_URL,
    CLIENT_ID,
    DEFAULT_PROXY_URL,
    DOWNLOAD_URL,
    L1C_COLLECTION,
    L1C_PRODUCT_TYPE,
    L2A_PRODUCT_TYPE,
    SEARCH_CHUNK_DAYS,
    TOKEN_URL,
)

from .auth import CdseCredentials, CdseTokenProvider
from .client import CdseODataClient
from .download import ODataProductDownloader
from .search import ODataProductSearcher
from .service import CdseService


def build_cdse_service() -> CdseService:
    """Создаёт CDSE service из окружения и production adapters."""
    username = os.environ.get("CDSE_USERNAME")
    password = os.environ.get("CDSE_PASSWORD")
    if not username or not password:
        raise RuntimeError("Не заданы CDSE_USERNAME / CDSE_PASSWORD")

    credentials = CdseCredentials(
        username=username,
        password=password,
        client_id=CLIENT_ID,
        totp=os.environ.get("CDSE_TOTP"),
    )
    session = requests.Session()
    if DEFAULT_PROXY_URL:
        session.proxies.update(
            {
                "http": DEFAULT_PROXY_URL,
                "https": DEFAULT_PROXY_URL,
            }
        )
    token_provider = CdseTokenProvider(
        credentials,
        session=session,
        token_url=TOKEN_URL,
    )
    client = CdseODataClient(
        token_provider,
        catalogue_base=API_URL,
        download_base=DOWNLOAD_URL,
        session=session,
    )
    return CdseService(
        searcher=ODataProductSearcher(
            client,
            chunk_days=SEARCH_CHUNK_DAYS,
        ),
        downloader=ODataProductDownloader(client),
        fallback_collection=L1C_COLLECTION,
        preferred_product_type=L2A_PRODUCT_TYPE,
        fallback_product_type=L1C_PRODUCT_TYPE,
    )
