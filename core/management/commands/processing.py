"""Команда по полному циклу поиска и обработки изображений."""
import json
import os
import sys
from collections import defaultdict
from typing import Dict, Tuple, List

from glob2 import glob

from core import settings
from core.logging import get_logger
from core.management.base import BaseCommand
from core.utils import remove_files_from_dir, copy_zip_to_archive, \
    parse_zip_name, iter_zip_files
from core.zip.handlers import ZipHandler
from processing.processors import *
from processing.processors.tiles import execute_tile_image_processor
from satgeo.public import execute_publisher


class Command(BaseCommand):
    """
    Команда processing.

    Поддерживает два режима:
    - обычный: берёт ZIP'ы из settings.DOWNLOADS_DIR (по умолчанию)
    - архивный: --archive, берёт ZIP'ы из архива
    (по умолчанию /mnt/map/Snapshots или --archive-root)
    При падении/перезагрузке команда
    продолжит с места остановки.
    """
    help = "Команда полного цикла обработки спутниковых изображений."

    def __init__(self):
        super().__init__()
        self.logger = get_logger(name="Processing Sentinel-2")

    def add_arguments(self, parser):
        parser.add_argument(
            "-d", "--debug", action="store_true",
            help="Режим разработчика. "
                 "В режиме разработки не удаляются файлы из отработанных папок"
        )
        parser.add_argument(
            "--archive", action="store_true",
            help="Включить режим чтения исходников из архива "
                 "(вместо DOWNLOADS_DIR)."
        )
        parser.add_argument(
            "--archive-root",
            type=str,
            default=getattr(
                settings, "SNAPSHOTS_ARCHIVE_ROOT", "/mnt/map/Snapshots"
            ),
            help="Корневая директория архива "
                 "(по умолчанию /mnt/map/Snapshots или "
                 "settings.SNAPSHOTS_ARCHIVE_ROOT)."
        )
        parser.add_argument(
            "--start-year", type=int, default=2010,
            help="Год, с которого начинать обход архива (по умолчанию 2010)."
        )

    def handle(self, *args, **options):
        archive_mode = options.get("archive", False)
        archive_root = options.get("archive_root")
        start_year = options.get("start_year", 2010)
        debug = options.get("debug", False)

        if archive_mode:
            self.run_from_archive(archive_root, start_year, debug)
        else:
            self.run(debug=debug)

    def run(self, debug: bool = False):
        zip_files = glob(os.path.join(settings.DOWNLOADS_DIR, '*.zip'))
        if not zip_files:
            self.logger.warning(
                "В директории download спутниковых снимков в формате '*.zip' "
                "не обнаружено. Поместите необходимые снимки в эту папку вручную."
            )
            sys.exit(-1)

        # Обрабатываем каждый снимок в цикле
        for zip_file in zip_files:
            self._process_zip(zip_file)

        # Публикация спутниковых снимков на Geoserver
        execute_publisher()

        # Удаление отработанных файлов
        if not debug:
            self._clean()

    def run_from_archive(
            self,
            archive_root: str,
            start_year: int,
            debug: bool
    ):
        """
        Обход архива, формирование пар ULA+ULB одной даты и поочередная обработка.
        Поддерживает resume через JSON state.
        """

        self.logger.info("Строим список пар ULA+ULB из архива")

        archive_root = archive_root or getattr(
            settings,
            "SNAPSHOTS_ARCHIVE_ROOT",
            "/mnt/map/Snapshots"
        )

        state_path = getattr(
            settings,
            "PROCESS_STATE_FILE",
            os.path.join(
                getattr(settings, "BASE_DIR", "."),
                "processing_state.json"
            )
        )

        pairs_map: Dict[Tuple[int, str], Dict[str, str]] = defaultdict(dict)

        self.logger.info("Сканируем архив...")

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

        self.logger.info(f"Найдено ZIP файлов: {zip_count}")

        all_pairs: List[Tuple[int, str, str, str]] = []

        for (year, date_key), tiles in pairs_map.items():

            ula_path = next(
                (path for tile, path in tiles.items() if tile.endswith("ula")),
                None
            )
            ulb_path = next(
                (path for tile, path in tiles.items() if tile.endswith("ulb")),
                None
            )

            if ula_path and ulb_path:
                all_pairs.append((year, date_key, ula_path, ulb_path))

        all_pairs.sort(key=lambda x: (x[0], x[1]))

        self.logger.info(f"Найдено {len(all_pairs)} валидных пар ULA+ULB")

        if not all_pairs:
            self.logger.warning(
                "Нет ни одной полной пары ULA+ULB для обработки"
            )
            return

        state = self._load_state(state_path)
        start_index = 0

        if state:
            # если оборвались посередине партии
            if state.get("status") == "processing":
                last = state.get("current_pair")
                if last:
                    self.logger.warning(
                        "Обнаружено падение внутри партии → повторяем партию"
                    )
                    for idx, (y, d, _, _) in enumerate(all_pairs):
                        if y == last["year"] and d == last["date"]:
                            start_index = idx
                            break

            # если последняя партия успешно завершена
            elif state.get("status") == "done":
                last_done = state.get("last_done")
                if last_done:
                    for idx, (y, d, _, _) in enumerate(all_pairs):
                        if (y > last_done["year"]) or (
                                y == last_done["year"] and d > last_done[
                            "date"]
                        ):
                            start_index = idx
                            break

        self.logger.info(f"Старт обработки с позиции {start_index}")

        for idx in range(start_index, len(all_pairs)):
            year, date_key, zip_ula, zip_ulb = all_pairs[idx]

            self.logger.info(
                f"Партия {idx + 1}/{len(all_pairs)} → {year} {date_key}"
            )

            self._save_state(state_path, {
                "status": "processing",
                "current_pair": {"year": year, "date": date_key}
            })

            try:
                ula_bool = self._process_zip(zip_ula, from_archive=True)
                ulb_bool = self._process_zip(zip_ulb, from_archive=True)

                if not ula_bool or not ulb_bool:
                    self.logger.warning(
                        "Не удалось обработать пару ULA+ULB, пропускаем публикацию..."
                    )
                    if not debug:
                        self._clean()
                    continue

                # Публикация спутниковых снимков на Geoserver
                execute_publisher()

                if not debug:
                    self._clean()

                self._save_state(state_path, {
                    "status": "done",
                    "last_done": {"year": year, "date": date_key}
                })

                self.logger.info(f"Пара обработана успешно: {year} {date_key}")

            except Exception as exc:
                self.logger.exception(
                    f"Ошибка партии {year} {date_key}: {exc}")
                return

        self.logger.info("Обработка архива завершена полностью.")

    def _process_zip(self,
                     zip_file: str,
                     from_archive: bool = False) -> bool:
        """Полный цикл обработки архива со снимком."""
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
        tile = kwargs["tile"]

        agroids = [1, 3, 4] if tile == "t38ula" else [1, 5, 6]

        # Перенос исходного zip-файла в архив
        if not from_archive:
            copy_zip_to_archive(year, tile_name, zip_file)

        # Разархивация снимка в temp_processing_dir
        needed_files = ["TCI", "SCL", "B03", "B04", "B08"]
        levels = {"msil1c": "L1C", "msil2a": "L2A"}
        zip_file_path = zip_obj.unzip(
            dst_path=settings.TEMP_PROCESSING_DIR,
            needed_files=needed_files,
            level=levels[level]
        )

        if not os.path.exists(zip_file_path):
            self.logger.warning("Битый ZIP-файл, пропускаем...")
            return False

        # Выборка снимков и построение индексов
        execute_tile_image_processor(**kwargs)
        #
        # # Нарезка изображений по агропредприятиям
        # execute_sentinel_image_processor(agroids=agroids, **kwargs)
        #
        # # Объединение изображений агропредприятий (в частности для agroid=1)
        # execute_combine_image_processor(**kwargs)
        #
        # # Расчёт NDVI по полям, только для уровня L2A обработки
        # if level == "msil2a":
        #     execute_cloud_mask_image_processor(agroids=agroids, **kwargs)
        #     execute_ndvi_statistics_image_processor(**kwargs)

        return True

    @staticmethod
    def _clean():
        remove_files_from_dir(
            settings.INTERMEDIATE,
            settings.PROCESSED_DIR,
            settings.NDVI_DIR,
            settings.TEMP_PROCESSING_DIR,
            settings.DOWNLOADS_DIR
        )

    def _load_state(self, path: str) -> Dict:
        """Загружаем state JSON (если есть). Возвращает dict или {}."""
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as fh:
                    return json.load(fh) or {}
        except Exception as exc:
            self.logger.warning(
                "Не удалось загрузить state из JSON: %s", exc
            )
        return {}

    def _save_state(self, path: str, state: Dict):
        """Атомарно сохраняем state в JSON-файл."""
        tmp = f"{path}.tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        except Exception as exc:
            self.logger.warning(
                "Не удалось создать директорию: %s", exc
            )
            pass
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
