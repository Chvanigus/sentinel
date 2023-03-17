""" Утилиты для проекта"""
import ftplib
import re
import zipfile
from datetime import datetime
from ftplib import FTP
from os import makedirs, remove
from os.path import basename, exists, join
from shutil import rmtree

import psycopg2
from glob2 import glob


class DBConnector:
    """ Диспетчер контекста для подключения к базе данных.
        Параметры подключения передаются через словарь
    """

    def __init__(self, config: dict) -> None:
        self.configuration = config

    def __enter__(self) -> psycopg2:
        """ Для подключения к базе данных используется библиотека psycopg2
        :return:
            Объект подключения к базе данных (курсор)
        """
        self.conn = psycopg2.connect(**self.configuration)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_trace) -> None:
        """ При выходе происходит сохранение всех изменений и закрытие подключения"""
        self.conn.commit()  # Сохранение изменений в базе
        self.cursor.close()
        self.conn.close()


def remove_processed_files(*args: str) -> None:
    """ Удаление всех файлов в заданных директориях

    :param args:
        Пути к директориям
    """
    for arg in args:
        files = glob(join(arg, '*'))
        for file in files:
            try:
                try:
                    remove(file)
                except IsADirectoryError:
                    try:
                        rmtree(file)
                    except OSError as e:
                        print(f'Ошибка при удалении файла: {e}')
                        pass
            except OSError as e:
                print(f'Не удалось удалить файл. Пропуск. Ошибка: {e}')
                pass


def get_filename(file_path: str) -> str:
    """ Возвращает название файла без расширения

    :param file_path:
        Путь к файлу
    :returns:
        Название файла
    :example:
        >>> get_filename('/path/to/my_image.jpg')
        my_image
    """
    return (basename(file_path)).split('.')[0]


def save_archive_to_remote_server(src_path: str, ip_server: str = '192.168.0.17', port_server: int = 11121,
                                  username: str = 'MIlmensky', password: str = 'm7f6k4W@a1',
                                  dst_path: str = '/Sentinel Archive/') -> None:
    """ Сохраняет файлы на удаленный сервер через FTP

    :param src_path:
        Входной файл
    :param ip_server:
        IP сервера
    :param port_server:
        Порт подключения
    :param username:
        Имя пользователя
    :param password:
        Пароль
    :param dst_path:
        Путь сохранения файла
    """
    ftp = FTP()
    with open(src_path, 'rb') as file:
        try:
            ftp.connect(host=ip_server, port=port_server)
            ftp.login(user=username, passwd=password)
            ftp.storbinary(f'STOR {dst_path + basename(src_path)}', file)
            ftp.quit()
            print('Архив сохранён успешно')
        except ftplib.all_errors as e:
            print(f'Невозможно сохранить архив на удаленный сервер. Пропуск. Ошибка: {e}')
            pass


def split_file_name(layer_name: str) -> tuple:
    """ Разбивает строку названия файла на базовые составляющие

        :param layer_name:
            Название файла
        :example:
            >>> split_file_name('path/to/s2a_19_07_2021_a4_rgb_10m_3857.tif')
            19_07_2021 rgb 10 4 None s2a_19_07_2021_a4_rgb_10m_3857 s2a
        :return:
            Кортеж строк
    """

    names = get_filename(layer_name).split('_')
    field_id = None  # Номер поля
    date = re.search(r'\d{2}_\d{2}_\d{4}', layer_name).group()  # Дата снимка
    satellite = names[0]  # Название спутника
    image_set = names[5]  # Тип изображения (rgb или ndvi)
    resolution = re.sub('[^0-9]', '', names[6])  # Разрешение (по дефолту - 10м на пиксель)
    field_group_id = re.sub('[^0-9]', '', names[4]) if names[4] != 'None' else None  # Номер Агро

    return date, image_set, resolution, field_group_id, field_id, layer_name.split('.')[0], satellite


def check_create_folder(folder_path: str) -> str:
    """ Проверяет, существует ли папка. Если нет - создает папку

    :param folder_path:
        Путь к папке
    :returns:
        Путь к папке
    """
    if not exists(folder_path):
        makedirs(folder_path)

    return folder_path


def check_positive_int(s: any) -> int:
    """ Функция проверки значения на положительность
        :param s:
            Входное значение
        :return:
            Целочисленное положительное значение
    """
    msg = f'Недействительное положительное значение int: {s}.'
    try:
        ivalue = int(s)
        if ivalue <= 0:
            raise ValueError(msg)
        return ivalue
    except ValueError:
        raise ValueError(msg)


def get_data_from_archive_sentinel(file_path: str) -> tuple:
    """ Разбивает строку названия файла на базовые составляющие (архив sentinel)
        :param file_path:
            Обрабатываемый архив
        :return:
            Кортеж строк
        :example:
            >>> get_data_from_archive_sentinel('path/S2A_MSIL1C_20210719T081611_N0301_R121_T38ULA_20210719T091337.zip')
            S2A 2021-07-19 T38ULA
    """
    name_file = get_filename(file_path)
    satellite = re.match(r'S[1-3][A-B]', name_file).group()  # Номер и буква спутника
    date = datetime.strptime(re.search(r'202\d{5}', name_file).group(), '%Y%m%d').date()  # Дата снимка
    tile = re.search(r'[A-Z]\d{2}[A-Z]{3}', name_file).group()  # Номер квадрата

    return satellite, date, tile


def unzip(src_path: str, dst_path: str) -> None:
    """ Распаковывает zip файл в нужную директорию
    :param src_path:
        Путь к zip файлу
    :param dst_path:
        Выходная директория данных из архива
    """
    try:
        file = zipfile.ZipFile(src_path)
        file.extractall(dst_path)
    except zipfile.error:
        pass
