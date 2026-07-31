"""Настройки процесса, CDSE и GeoServer из переменных окружения."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


def _path(name: str, default: str | Path) -> str:
    """Читает путь из окружения и раскрывает домашний каталог пользователя."""
    return str(Path(os.environ.get(name, str(default))).expanduser())


# Processing filesystem and GIS.
DOWNLOADS_DIR = _path("DOWNLOADS_DIR", BASE_DIR / "downloads")
TEMP_PROCESSING_DIR = _path("TEMP_PROCESSING_DIR", BASE_DIR / "temp")
INTERMEDIATE = _path("INTERMEDIATE_DIR", BASE_DIR / "intermediate")
PROCESSED_DIR = _path("PROCESSED_DIR", BASE_DIR / "processed")
NDVI_DIR = _path("NDVI_DIR", BASE_DIR / "ndvi")
ARCHIVE_ROOT = _path("ARCHIVE_ROOT", "/mnt/map/Snapshots")
DESTSRID = int(os.environ.get("DESTSRID", "3857"))
NODATA = float(os.environ.get("NODATA", "-9999"))
YEAR = datetime.now().year

# Copernicus Data Space Ecosystem.
TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
    "protocol/openid-connect/token"
)
API_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
DOWNLOAD_URL = "https://download.dataspace.copernicus.eu/odata/v1"
CLIENT_ID = "cdse-public"
L2A_COLLECTION = "SENTINEL-2"
L1C_COLLECTION = "SENTINEL-2"
L2A_PRODUCT_TYPE = "S2MSI2A"
L1C_PRODUCT_TYPE = "S2MSI1C"
PAGE_LIMIT = 500
SEARCH_CHUNK_DAYS = 1
DEFAULT_PROXY_URL = os.environ.get("CDSE_PROXY")
_target_tiles = os.environ.get("CDSE_TILES") or os.environ.get(
    "TILES",
    "38ULA,38ULB",
)
TARGET_TILES = [
    tile.strip() for tile in _target_tiles.split(",") if tile.strip()
]

# GeoServer.
GS_DATA_ROOT = _path("GS_DATA_ROOT", "/mnt/map/geoware")
GS_DATA_DIR = _path("GS_DATA_DIR", "/opt/geoserver_data/geoware")
GS_HOST = os.environ.get("GS_HOST", "localhost")
GS_WORKSPACE = os.environ.get("GS_WORKSPACE", "sentinel")
GS_USERNAME = os.environ.get("GS_USERNAME", "admin")
GS_PASSWORD = os.environ.get("GS_PASSWORD", "")


def get_archive_dir(year: str | int, tile: str) -> str:
    """Возвращает каталог архива для года и нормализованного тайла."""
    normalized_tile = tile.strip().upper().removeprefix("T")
    return str(Path(ARCHIVE_ROOT) / str(year) / normalized_tile)
