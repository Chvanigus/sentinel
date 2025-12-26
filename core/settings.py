"""Файл настроек проекта."""
import datetime
import os
from os.path import expanduser, join
from pathlib import Path

from dotenv import load_dotenv

# загружаем .env
load_dotenv()

# Домашняя директория пользователя
HOME_DIR = expanduser("~")

# Директория проекта
BASE_DIR = Path(__file__).resolve().parent.parent

# Режим разработки
DEBUG = os.environ.get("DEBUG", "False").lower() == "true"

# Директория скаченных снимков
DOWNLOADS_DIR = join(BASE_DIR, "downloads")
# Временная директория для обработки снимков
TEMP_PROCESSING_DIR = join(BASE_DIR, "temp")
# Директория для второго этапа обработки
INTERMEDIATE = join(BASE_DIR, "intermediate")
# Директория обработанных снимков
PROCESSED_DIR = join(BASE_DIR, "processed")
# Директория обработки NDVI индекса
NDVI_DIR = join(BASE_DIR, "ndvi")
# Директория комбинирования изображений
COMBINE_DIR = join(BASE_DIR, "combine")
# Директория с расцветками снимков
STYLES_DIR = join(BASE_DIR, "works_files", "styles_qgis")

# SRID проекция для обработки
DESTSRID_OBTAIN = int(os.environ.get("DESTSRID_OBTAIN", 4326))
# SRID проекция для публикации снимков
DESTSRID = int(os.environ.get("DESTSRID", 3857))

# Квадраты, в которых расположены агропредприятия
tiles_env = os.environ.get("TILES", "38ULA,38ULB")
TILES = [x.strip() for x in tiles_env.split(",") if x.strip()]

# Текущий год
YEAR = str(datetime.datetime.now().year)

# Значение, которое принимают невалидные пиксели
NODATA = -9999

# Путь для публикации спутникового снимка
IMAGE_DIR = "/mnt/map/geoware/SENTINEL" + YEAR + "/"

# SSH/Remote Machine
RMHOST = os.environ.get("RMHOST")
RMUSER = os.environ.get("RMUSER")
RMPASSWORD = os.environ.get("RMPASSWORD")
SSH_PORT = int(os.environ.get("SSH_PORT", 22))

# TileServer / GeoServer auth
TSUSER = os.environ.get("TSUSER")
TSPASSWORD = os.environ.get("TSPASSWORD")
TSPORT = int(os.environ.get("TSPORT", 11990))
WORKSPACE = os.environ.get("WORKSPACE", "sentinel")
TILE_SIZE = int(os.environ.get("TILE_SIZE", 128))

USE_GWC = os.environ.get("USE_GWC", "True").lower() == "true"
ZOOM_START = int(os.environ.get("ZOOM_START", 7))
ZOOM_STOP = int(os.environ.get("ZOOM_STOP", 16))

# SentinelHub
SH_CLIENT_ID = os.environ.get("SH_CLIENT_ID")
SH_CLIENT_SECRET = os.environ.get("SH_CLIENT_SECRET")

SH_USERNAME = os.environ.get("SH_USERNAME")
SH_PASSWORD = os.environ.get("SH_PASSWORD")

# Архив
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "/mnt/map/geoware/SENTINEL")


def get_archive_dir(year: str, tile: str) -> str:
    """Возвращает путь к архиву снимка."""
    return f"/mnt/map/Snapshots/{year}{tile}/"


HEADERS_XML = {
    "Content-type": "application/xml",
    "Accept": "application/xml"
}