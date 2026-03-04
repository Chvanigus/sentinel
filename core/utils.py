"""Утилиты проекта SENTINEL."""
import os
import re
import shutil
from datetime import datetime
from os import makedirs
from os.path import exists, join
from typing import Optional, Tuple

from glob2 import glob

from core import settings
from core.logging import get_logger

ZIP_PATTERN = re.compile(
    r".*_(\d{8})T\d{6}.*_(T\d{2}[A-Z]{3})_.*\.zip$"
)


def remove_files_from_dir(*args: str) -> None:
    """
    Удаление всех файлов в заданных директориях
    :param args: Пути к директориям
    """
    logger = get_logger(__name__)
    for arg in args:
        files = glob(join(arg, "*"))
        logger.info("Удаление файлов: %s из %s", files, arg)
        for file in files:
            try:
                try:
                    os.remove(file)
                    logger.info(f"Удалён файл: %s", file)
                except IsADirectoryError:
                    try:
                        shutil.rmtree(file)
                        logger.info(f"Удалён каталог: %s", file)
                    except OSError as e:
                        logger.info(f"Ошибка при удалении файла: %s", e)
                        pass
            except OSError as e:
                logger.info(f"Не удалось удалить файл. Пропуск. Ошибка: %s", e)
                pass


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
    logger = get_logger("core.utils.copy_zip_to_archive")
    folder_absolute_path = settings.get_archive_dir(
        year=str(year),
        tile=tile_name,
    )

    check_create_folder(folder_absolute_path)

    zip_filename = os.path.basename(zip_file)
    destination_path = os.path.join(folder_absolute_path, zip_filename)

    if os.path.exists(destination_path):
        logger.info(
            "zip файл уже уже существует в архиве, пропуск копирования: %s",
            destination_path,
        )
        return

    logger.info(
        "Перенос zip файла со снимком в архив снимков: %s -> %s",
        zip_file,
        destination_path,
    )
    shutil.copy2(zip_file, destination_path)


def parse_zip_name(zip_path: str) -> Optional[Tuple[str, str]]:
    """
    Быстро вытаскивает дату и tile из имени ZIP.

    Пример:
    S2A_MSIL1C_20210117T081301..._T38ULB_....zip
    → ("20210117", "t38ulb")
    """
    filename = os.path.basename(zip_path)

    match = ZIP_PATTERN.match(filename)
    if not match:
        return None

    date_key = match.group(1)  # 20210117
    tile_key = match.group(2).lower()  # t38ulb

    return date_key, tile_key


def iter_zip_files(root: str):
    """
    Быстрый рекурсивный обход архива через os.scandir.
    """
    stack = [root]

    while stack:
        current_dir = stack.pop()

        try:
            with os.scandir(current_dir) as entries:
                for entry in entries:
                    if entry.is_dir():
                        stack.append(entry.path)

                    elif entry.is_file() and entry.name.endswith(".zip"):
                        yield entry.path

        except PermissionError:
            continue
