"""Утилиты проекта SENTINEL."""
import os
import re
import shutil
from datetime import datetime
from os import makedirs
from os.path import exists, join

from glob2 import glob

from core import settings
from core.logging import get_logger

logger = get_logger()


def remove_files_from_dir(*args: str) -> None:
    """
    Удаление всех файлов в заданных директориях
    :param args: Пути к директориям
    """
    logger = get_logger()
    for arg in args:
        files = glob(join(arg, "*"))
        logger.info(f"Удаление файлов: {files} из {arg}")
        for file in files:
            try:
                try:
                    os.remove(file)
                    logger.info(f"Удалён файл: {file}")
                except IsADirectoryError:
                    try:
                        shutil.rmtree(file)
                        logger.info(f"Удалён каталог: {file}")
                    except OSError as e:
                        logger.info(f"Ошибка при удалении файла: {e}")
                        pass
            except OSError as e:
                logger.info(f"Не удалось удалить файл. Пропуск. Ошибка: {e}")
                pass


def split_file_name(layer_name: str) -> tuple:
    """
    Разбивает строку названия файла на базовые составляющие
    :param layer_name: Название файла.
    :return: Кортеж строк
    """
    names = get_basename(layer_name).split("_")
    field_id = None  # Номер поля
    date = re.search(r"\d{2}_\d{2}_\d{4}", layer_name).group()  # Дата снимка
    satellite = names[0]  # Название спутника
    image_set = names[5]  # Тип изображения (rgb или ndvi)
    resolution = re.sub("[^0-9]", "", names[6])  # Разрешение
    field_group_id = re.sub(
        "[^0-9]", "", names[4]
    ) if names[4] != "None" else None  # Номер Агро

    return date, image_set, resolution, field_group_id, field_id, \
        layer_name.split(".")[0], satellite


def check_create_folder(folder_path: str) -> str:
    """
    Проверяет, существует ли папка. Если нет - создает папку
    :param folder_path: Путь к папке
    :returns: Путь к папке
    """
    if not exists(folder_path):
        try:
            makedirs(folder_path)
        except PermissionError:
            pass

    return folder_path


def get_basename(filename) -> str:
    """
    Возвращает название файла без путей.
    :param filename: Путь к файлу.
    :return: Базовое название файла.
    """
    return os.path.basename(filename)


def get_date_obj(date) -> datetime.date:
    """Возвращает объект datetime.date из строки формата 'DD_MM_YYYY'."""
    return datetime.strptime(date, "%d_%m_%Y").date()

def copy_zip_to_archive(
        year: str, tile_name: str, zip_file
) -> None:
    """Копирует zip файл в архив со снимками."""
    folder_absolute_path = settings.get_archive_dir(
        year=str(year), tile=tile_name
    )

    check_create_folder(folder_absolute_path)
    logger.info(f"Перенос архива со снимком в архив снимков {zip_file}")
    shutil.copy(zip_file, folder_absolute_path)
