"""Класс для работы с ZIP файлами."""
import os
import zipfile
from collections import namedtuple
from typing import Optional, Type

from core.logging import get_logger
from core.utils import get_basename
from core.zip.parsers import ZipParser


class ZipHandler:
    """Класс для работы с ZIP файлами."""

    def __init__(self, file: str):
        self.filename = file
        self.basename = self.get_basename()
        self.logger = get_logger("ZipHandler")

    def get_basename(self) -> str:
        """Возвращает название архива без путей."""
        return get_basename(self.filename)

    def get_zip_info(self) -> Type[namedtuple]:
        """
        Возвращает данные по архиву.
        :return: Возвращает именованный кортеж данных по архиву
                 (satellite, date, tile)
        """
        return ZipParser(self.basename).get_info()

    def get_zip_name(self) -> str:
        """Возвращает название архива."""
        return self.basename

    def unzip(self,
              dst_path: str,
              level: str = "L1C",
              needed_files: Optional[list[str]] = None) -> Optional[str]:
        """
        Потоковая распаковка ZIP только нужных файлов.
        :param dst_path: директория для распаковки
        :param needed_files: список имен файлов или шаблонов для извлечения (без пути)
        :param level: уровень данных (L1C, L2A)
        """

        level = level.upper()

        # Надёжно формируем имя .SAFE (учитываем регистр и любые расширения)
        base_no_ext = os.path.splitext(self.basename)[0]
        full_dst_path = os.path.join(dst_path, base_no_ext + ".SAFE")
        if os.path.exists(full_dst_path):
            self.logger.info("Архив уже распакован: %s", full_dst_path)
            return full_dst_path

        if not os.path.exists(self.filename) or os.path.getsize(
                self.filename) == 0:
            self.logger.error("ZIP файл не найден или пустой: %s",
                              self.filename)
            return None

        if not zipfile.is_zipfile(self.filename):
            self.logger.error("Файл не является ZIP или повреждён: %s",
                              self.filename)
            return None

        extracted_count = 0
        try:
            with zipfile.ZipFile(self.filename, "r") as zip_file:
                for member in zip_file.infolist():

                    # Обычно JP2 находятся в пути .../IMG_DATA/...
                    if "IMG_DATA" not in member.filename:
                        continue

                    name = os.path.basename(member.filename)
                    if not name.lower().endswith(".jp2"):
                        continue

                    # логика по уровням
                    if level == "L1C":
                        if needed_files:
                            matched = False
                            lname = name.lower()
                            for f in needed_files:
                                lf = f.lower()
                                if lf == "scl":
                                    continue
                                if f"_{lf}_" in lname or lname.endswith(
                                        f"_{lf}.jp2") or (
                                        f"_{lf}_" in member.filename.lower()):
                                    matched = True
                                    break
                            if not matched:
                                continue

                    elif level == "L2A":
                        if needed_files:
                            matched = False
                            lname = name.lower()
                            for f in needed_files:
                                lf = f.lower()
                                if f"_{lf}_" in lname or lname.endswith(
                                        f"_{lf}.jp2") or (
                                        f"_{lf}_" in member.filename.lower()):
                                    matched = True
                                    break
                            if not matched:
                                continue

                        if "_scl_" in name.lower():
                            if "R20m" not in member.filename:
                                continue
                        else:
                            if "R10m" not in member.filename:
                                continue

                    else:
                        self.logger.error("Неизвестный уровень данных: %s",
                                          level)
                        return None

                    target_path = os.path.join(dst_path, member.filename)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)

                    with zip_file.open(member) as source, open(target_path,
                                                               "wb") as target:
                        while chunk := source.read(16 * 1024 * 1024):
                            target.write(chunk)

                    extracted_count += 1

            if extracted_count == 0:
                self.logger.warning(
                    "Ничего не распаковано (файлы отфильтрованы). Проверьте needed_files и шаблоны имен в ZIP: %s",
                    self.filename)
                return None

            self.logger.info(
                "Архив успешно распакован в %s (извлечено %d файлов)",
                full_dst_path, extracted_count)
            return full_dst_path

        except zipfile.BadZipFile as e:
            self.logger.error("Файл повреждён или не является ZIP: %s, %s",
                              self.filename, e)
            return None

        except Exception as err:
            self.logger.error("Не удалось распаковать архив %s: %s",
                              self.filename, err)
            return None
