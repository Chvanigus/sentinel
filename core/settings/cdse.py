"""Настройки Copernicus Data Space Ecosystem."""
from __future__ import annotations

TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
API_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
DOWNLOAD_URL = "https://download.dataspace.copernicus.eu/odata/v1"

CLIENT_ID = "cdse-public"

S2_COLLECTION = "SENTINEL-2"
L2A_COLLECTION = S2_COLLECTION
L1C_COLLECTION = S2_COLLECTION

L2A_PRODUCT_TYPE = "S2MSI2A"
L1C_PRODUCT_TYPE = "S2MSI1C"

# Тайлы можно задавать с T-префиксом.
TARGET_TILES = ["T38ULA", "T38ULB"]

# Размер страницы OData.
PAGE_LIMIT = 500

# Дробим большой период на маленькие интервалы.
SEARCH_CHUNK_DAYS = 1

# Если у тебя локальный SOCKS-прокси, оставь так.
DEFAULT_PROXY_URL = "socks5h://127.0.0.1:10808"