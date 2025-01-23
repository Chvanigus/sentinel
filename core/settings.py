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

DESTSRID_OBTAIN = 4326
DESTSRID = 3857

# Интервал допустимых годов
YEAR_INTERVAL = [1900, datetime.datetime.now().year + 5]

CLOUD_MAX = 100

TILES = ['38ULA', '38ULB']

if platform.system() == 'Linux':
    SEN2COR_PATH = join(HOME_DIR, 'Sen2Cor-02.11.00-Linux64', 'bin', 'L2A_Process')
else:
    SEN2COR_PATH = join('D:\\', 'sen2cor', 'L2A_Process')

# Текущий год
YEAR = str(datetime.datetime.now().year)

COLORMAP = join(BASE_DIR, 'works_files', 'styles_qgis', 'ndvi_style.qml')

NODATA = -9999

db_config = {'host': '192.168.0.9',
             'user': 'postgres',
             'password': 'Maryland2017',
             'database': 'gpgeo'}

IMAGE_DIR = '/mnt/geoware/SENTINEL' + str(datetime.datetime.now().year) + '/'

RMHOST = '192.168.0.9'
RMUSER = 'sysop'
RMPASSWORD = 'm7f6k4W@a1'
SSH_PORT = 8822

TSUSER = 'admin'
TSPASSWORD = 'm7f6k4W@a1'
TSPORT = 11990
WORKSPACE = 'sentinel'
TILE_SIZE = 128

USE_GWC = True
ZOOM_START = 7
ZOOM_STOP = 16

DEBUG = False

ALLOWED_CLOUDS = 0  # Процент видимости полей
