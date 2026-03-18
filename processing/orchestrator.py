"""
Оркестратор полного цикла обработки спутниковых снимков (ULA+ULB).

Этот модуль содержит класс SentinelProcessingOrchestrator, который
инкапсулирует всю бизнес-логику обхода архива/папки, формирования пар,
resume/state, вызова процессоров, публикации и очистки.
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from typing import Dict, Tuple, List, Optional

from glob2 import glob

from core import settings
from core.logging import get_logger
from core.utils import (remove_files_from_dir,
                        copy_zip_to_archive,
                        parse_zip_name,
                        iter_zip_files)
from core.zip.handlers import ZipHandler
from processing.processors.tiles import execute_tile_image_processor
from satgeo.public import execute_publisher

logger = get_logger("ProcessingOrchestrator")


class SentinelProcessingOrchestrator:
    """
    Оркестратор обработки снимков.

    Публичные методы:
    - run(downloads_dir, debug=False)
    - run_from_archive(archive_root, start_year=2010, debug=False)
    """

    def __init__(self, *,
                 downloads_dir: Optional[str] = None,
                 archive_root_default: Optional[str] = None,
                 state_file: Optional[str] = None):
        """
        Инициализация оркестратора.

        :param downloads_dir: директория с ZIP (если None — берётся settings.DOWNLOADS_DIR)
        :param archive_root_default: дефолтный архив (если None — settings.SNAPSHOTS_ARCHIVE_ROOT)
        :param state_file: файл состояния (если None — settings.PROCESS_STATE_FILE или BASE_DIR/processing_state.json)
        """
        self.downloads_dir = downloads_dir or getattr(settings,
                                                      "DOWNLOADS_DIR",
                                                      "downloads")
        self.archive_root_default = archive_root_default or getattr(settings,
                                                                    "SNAPSHOTS_ARCHIVE_ROOT",
                                                                    "/mnt/map/Snapshots")
        self.state_file = state_file or getattr(settings, "PROCESS_STATE_FILE",
                                                os.path.join(getattr(settings,
                                                                     "BASE_DIR",
                                                                     "."),
                                                             "processing_state.json"))
        self.logger = logger

    # ---------------- Public API ----------------

    def run(self, debug: bool = False):
        """
        Обычный режим: берём ZIP'ы из self.downloads_dir и обрабатываем.
        """
        zip_files = glob(os.path.join(self.downloads_dir, "*.zip"))
        if not zip_files:
            self.logger.warning(
                "В директории %s не найдено файлов '*.zip'. "
                "Поместите необходимые снимки в эту папку.",
                self.downloads_dir
            )
            sys.exit(-1)

        for zip_file in zip_files:
            try:
                self._process_zip(zip_file, from_archive=False)
            except Exception as exc:
                self.logger.exception(
                    "Ошибка при обработке %s: %s",
                    zip_file, exc
                )
                continue

        # Публикация и очистка
        execute_publisher()
        if not debug:
            self._clean()

    def run_from_archive(self, archive_root: Optional[str] = None,
                         start_year: int = 2010, debug: bool = False):
        """
        Обход архива: собираем пары ULA+ULB,
        поддерживаем resume через state file.
        """
        archive_root = archive_root or self.archive_root_default
        self.logger.info("Сканируем архив: %s", archive_root)

        pairs_map: Dict[Tuple[int, str], Dict[str, str]] = defaultdict(dict)
        zip_count = 0

        for zip_path in iter_zip_files(archive_root):
            zip_count += 1
            parsed = parse_zip_name(zip_path)
            if not parsed:
                continue
            date_key, tile_key = parsed
            year = int(date_key[:4])
            if year < start_year:
                continue
            pairs_map[(year, date_key)][tile_key] = zip_path

        self.logger.info(
            "Найдено ZIP файлов: %d",
            zip_count
        )

        all_pairs: List[Tuple[int, str, str, str]] = []
        for (year, date_key), tiles in pairs_map.items():
            ula_path = next((p for t, p in tiles.items() if t.endswith("ula")),
                            None)
            ulb_path = next((p for t, p in tiles.items() if t.endswith("ulb")),
                            None)
            if ula_path and ulb_path:
                all_pairs.append((year, date_key, ula_path, ulb_path))

        all_pairs.sort(key=lambda x: (x[0], x[1]))
        self.logger.info(
            "Найдено %d валидных пар ULA+ULB",
            len(all_pairs)
        )

        if not all_pairs:
            self.logger.warning(
                "Нет ни одной полной пары ULA+ULB для обработки")
            return

        state = self._load_state(self.state_file)
        start_index = 0

        if state:
            status = state.get("status")
            if status == "processing":
                last = state.get("current_pair")
                if last:
                    self.logger.warning(
                        "Обнаружено падение внутри партии → повторяем партию")
                    for idx, (y, d, _, _) in enumerate(all_pairs):
                        if y == last["year"] and d == last["date"]:
                            start_index = idx
                            break
            elif status == "done":
                last_done = state.get("last_done")
                if last_done:
                    for idx, (y, d, _, _) in enumerate(all_pairs):
                        if (y > last_done["year"]) or (
                                y == last_done["year"] and d > last_done[
                            "date"]):
                            start_index = idx
                            break

        self.logger.info(
            "Старт обработки с позиции %d",
            start_index
        )

        for idx in range(start_index, len(all_pairs)):
            year, date_key, zip_ula, zip_ulb = all_pairs[idx]
            self.logger.info(
                "Партия %d/%d → %s %s",
                idx + 1, len(all_pairs), year, date_key
            )

            self._save_state(
                self.state_file, {
                    "status": "processing",
                    "current_pair": {
                        "year": year,
                        "date": date_key
                    }
                }
            )

            try:
                ula_ok = self._process_zip(zip_ula, from_archive=True)
                ulb_ok = self._process_zip(zip_ulb, from_archive=True)

                if not ula_ok or not ulb_ok:
                    self.logger.warning(
                        "Не удалось обработать пару ULA+ULB, "
                        "пропускаем публикацию..."
                    )
                    if not debug:
                        self._clean()
                    continue

                execute_publisher()
                if not debug:
                    self._clean()

                self._save_state(
                    self.state_file, {
                        "status": "done",
                        "last_done": {
                            "year": year,
                            "date": date_key
                        }
                    }
                )
                self.logger.info(
                    "Пара обработана успешно: %s %s",
                    year, date_key
                )

            except Exception as exc:
                self.logger.exception(
                    "Ошибка партии %s %s: %s",
                    year, date_key, exc
                )
                return

        self.logger.info("Обработка архива завершена полностью.")

    # ---------------- Internal methods ----------------

    def _process_zip(self, zip_file: str, from_archive: bool = False) -> bool:
        """
        Полный цикл обработки одного zip-файла.
        Возвращает True при успешной обработке, False — при ошибке/пропуске.
        """
        self.logger.info(
            "Обработка ZIP: %s (from_archive=%s)",
            zip_file, from_archive
        )

        zip_obj = ZipHandler(zip_file)
        info = zip_obj.get_zip_info()
        level = info.level.lower()

        kwargs = {
            "date": info.date.strftime("%d_%m_%Y"),
            "tile": info.tile.lower(),
            "satellite": info.satellite.lower(),
            "level": level,
        }

        tile_name = info.tile[3:6].upper()
        year = info.date.year

        # Перенос zip в архив (если мы работаем из DOWNLOADS_DIR)
        if not from_archive:
            try:
                copy_zip_to_archive(year, tile_name, zip_file)
            except Exception as exc:
                self.logger.warning(
                    "Не удалось скопировать zip в архив: %s",
                    exc
                )

        # Разархивация в TEMP_PROCESSING_DIR
        needed_files = ["TCI", "SCL", "B03", "B04", "B08"]
        levels = {"msil1c": "L1C", "msil2a": "L2A"}
        try:
            zip_file_path = zip_obj.unzip(
                dst_path=settings.TEMP_PROCESSING_DIR,
                needed_files=needed_files,
                level=levels[level])
        except Exception as exc:
            self.logger.exception(
                "Ошибка при распаковке %s: %s", zip_file, exc
            )
            return False

        if not os.path.exists(zip_file_path):
            self.logger.warning(
                "Битый ZIP-файл: %s", zip_file
            )
            return False

        # Вызов процесса нарезки/индексации
        try:
            execute_tile_image_processor(**kwargs)
        except Exception as exc:
            self.logger.exception(
                "Ошибка при выполнении execute_tile_image_processor для %s: %s",
                zip_file, exc
            )
            return False

        return True

    @staticmethod
    def _clean():
        """
        Удаление промежуточных директорий.
        """
        remove_files_from_dir(
            settings.INTERMEDIATE,
            settings.PROCESSED_DIR,
            settings.NDVI_DIR,
            settings.TEMP_PROCESSING_DIR,
            settings.DOWNLOADS_DIR
        )

    def _load_state(self, path: str) -> dict:
        """
        Загружает state JSON (если есть). Возвращает dict или {}.
        """
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as fh:
                    return json.load(fh) or {}
        except Exception as exc:
            self.logger.warning(
                "Не удалось загрузить state из JSON: %s",
                exc
            )
        return {}

    def _save_state(self, path: str, state: dict):
        """
        Атомарно сохраняет state в JSON-файл.
        """
        tmp = f"{path}.tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        except Exception as exc:
            self.logger.warning(
                "Не удалось создать директорию: %s",
                exc
            )

        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
