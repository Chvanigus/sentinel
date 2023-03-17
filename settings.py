""" Файл настроек проекта"""
import datetime
import platform
from os.path import abspath, dirname, expanduser, join

# Домашняя директория пользователя
HOME_DIR = expanduser('~')
# Директория проекта
BASE_DIR = dirname(abspath(__file__))

# Внутренние директории проекта
# Директория скаченных снимков
DOWNLOADS_DIR = join(BASE_DIR, 'downloads')
# Временная директория для обработки снимков
TEMP_PROCESSING_DIR = join(BASE_DIR, 'temp_for_processing')
# Директория для второго этапа обработки
INPUT_DIR = join(BASE_DIR, 'intermediate')
# Директория обработанных снимков
PROCESSED_DIR = join(BASE_DIR, 'processed')
# Директория обработки NDVI индекса
NDVI_DIR = join(BASE_DIR, 'ndvi')
# Директория комбинирования изображений
COMBINE_DIR = join(BASE_DIR, 'combine')
# Директория скриптов обработки снимков
PROCESSING_DIR = join(BASE_DIR, 'processing')

# obtain_data
PATH_TO_SCRIPT_obtain_data = join(BASE_DIR, 'obtain_data', 'obtain_data.py')

# processing
PATH_TO_SCRIPT_sen2cor = join(PROCESSING_DIR, 'sen2cor.py')
PATH_TO_SCRIPT_cloud_mask = join(PROCESSING_DIR, 'cloudmask.py')
PATH_TO_SCRIPT_sentinel = join(PROCESSING_DIR, 'sentinel.py')
PATH_TO_SCRIPT_ndvivalue = join(PROCESSING_DIR, 'ndvivalue.py')
PATH_TO_SCRIPT_combine = join(PROCESSING_DIR, 'combining.py')

# public
PATH_TO_SCRIPT_public = join(BASE_DIR, 'public', 'public.py')

# Директория для загрузки
DOWNLOAD_DIR = join(BASE_DIR, 'downloads')

# Sentinel DataHub Логин/Пароль и ссылка на API URl.
USER_NAME = 'millergeo'
PASSWORD = 'Wialongeo'
API_URL = 'https://scihub.copernicus.eu/dhus'

# Идентификатор системы пространственной привязки (SRID) для перепроецирования изображений
DESTSRID_OBTAIN = 4326
DESTSRID = 3857

# Интервал допустимых годов
YEAR_INTERVAL = [1900, datetime.datetime.now().year + 5]

# Максимальный процент облачности для сцены. Значение по умолчанию
CLOUD_MAX = 100

# Номера квадратов в соответствии с соглашением об именах военно-сетевой справочной системы США (MGRS)
TILES = ['38ULA', '38ULB']

if platform.system() == 'Linux':
    SEN2COR_PATH = join(HOME_DIR, 'Sen2Cor-02.11.00-Linux64', 'bin', 'L2A_Process')
else:
    SEN2COR_PATH = join('D:\\', 'sen2cor', 'L2A_Process')

# Текущий год
YEAR = str(datetime.datetime.now().year)

# Путь к цветной схеме NDVI
COLORMAP = join(BASE_DIR, 'works_files', 'styles_qgis', 'ndvi_style.qml')

# Нет значения данных
NODATA = -9999

# Настройки для подключения к базе данных
db_config = {'host': '192.168.0.9',
             'user': 'geoadmin',
             'password': 'canopus',
             'database': 'gpgeo'}

# GeoServer image directory.
IMAGE_DIR = '/opt/geoware/SENTINEL' + str(datetime.datetime.now().year) + '/'

# For a remote tile server (GeoServer) set host, username and ssh port.
# Remote host address. Set to localhost if the tile server is installed on local system.
RMHOST = '192.168.0.9'
RMUSER = 'sysop'
# Password for remote host connection.
# It is highly recommended use public key instead of password authentication.
RMPASSWORD = 'm7f6k4W@a1'
# Remote host ssh port, default is 22.
SSH_PORT = 8822

# GeoServer settings.
TSUSER = 'admin'
TSPASSWORD = 'm7f6k4W@a1'
# GeoServer port, default is 8080.
TSPORT = 11990
WORKSPACE = 'sentinel'
TILE_SIZE = 128

# GeoWebCache settings.
# Configure GeoServer Geo Web Cache (GWC).
USE_GWC = True
# Cache zoom levels.
ZOOM_START = 7
ZOOM_STOP = 16

# Когда включен режим разработчика - не будет происходить удаление отработанных файлов, здесь аккуратно
DEBUG = False
