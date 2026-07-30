"""Оркестратор управления процессом обработки снимков."""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Callable, Any

import psycopg2

from core import settings
from core.logging import get_logger
from core.utils import parse_zip_name, iter_zip_files
from core.utils import remove_files_from_dir
from core.zip.handlers import ZipHandler
from db.connect_data import DSL
from db.db_class import get_postgis_worker
from processing import execute_sentinel_image_processor, \
    execute_combine_image_processor, execute_cloud_mask_image_processor, \
    execute_ndvi_statistics_image_processor
from processing.processors.tiles import execute_tile_image_processor
from satgeo.public import execute_publisher

logger = get_logger("ProcessingOrchestrator")


class SentinelProcessingOrchestrator:
    """
    Оркестратор обработки снимков.

    Обрабатывает архив, пропуская уже обработанные пары.
    """
    REQUIRED_AGROIDS: Tuple[int, ...] = (1, 3, 4, 5, 6)
    PROCESSED_ROOT = "/mnt/map/geoware"
    PROCESSED_TYPES: Tuple[str, ...] = ("ndvi", "ndwi", "tci")

    def __init__(self):
        self.archive_root_default = "/mnt/map/snapshots"
        self.logger = logger

    def run(
            self,
            archive_root: Optional[str] = None,
            debug: bool = False,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
    ):
        """
        Обрабатываем архив парами (ULA+ULB).

        В обычном режиме:
        - берём дату из имени архива;
        - проверяем наличие результатов по всем агро 1, 3, 4, 5, 6;
        - если хотя бы одного агро нет — обрабатываем дату;
        - если все агро уже обработаны — пропускаем.

        В debug-режиме проверка пропуска отключена полностью.
        """
        archive_root = archive_root or self.archive_root_default

        if not start_date and not end_date:
            self.logger.info("Фильтр дат не задан — обрабатываем всё")

        if start_date:
            self.logger.info("START DATE: %s", start_date)

        if end_date:
            self.logger.info("END DATE: %s", end_date)

        self.logger.info("Сканируем архив: %s", archive_root)

        pairs_map: Dict[Tuple[str, str], Dict[str, str]] = defaultdict(dict)

        for zip_path in iter_zip_files(archive_root):
            parsed = parse_zip_name(zip_path)
            if not parsed:
                continue

            date_key, tile_key = parsed
            tile_key = tile_key.lower()

            if tile_key.endswith("ula"):
                side = "ula"
                prefix = tile_key[:-3]
            elif tile_key.endswith("ulb"):
                side = "ulb"
                prefix = tile_key[:-3]
            else:
                continue

            pairs_map[(date_key, prefix)][side] = zip_path

        all_pairs: List[Tuple[str, str, str, str]] = []

        for (date_key, prefix), sides in pairs_map.items():
            if sides.get("ula") and sides.get("ulb"):
                all_pairs.append(
                    (date_key, prefix, sides["ula"], sides["ulb"]))

        all_pairs.sort(key=lambda x: x[0])

        self.logger.info("Найдено валидных пар: %d", len(all_pairs))

        filtered_pairs = []

        for date_key, prefix, ula, ulb in all_pairs:
            date_obj = datetime.strptime(date_key, "%Y%m%d")

            if start_date and date_obj < start_date:
                continue

            if end_date and date_obj > end_date:
                continue

            if not debug:
                with psycopg2.connect(**DSL) as pg_conn:
                    worker = get_postgis_worker(pg_conn)
                    missing = worker.get_missing_agroids(date_obj)

                if not missing:
                    self.logger.info(
                        "SKIP %s → всё есть (1,3,4,5,6)",
                        date_key
                    )
                    continue

                self.logger.info(
                    "PROCESS %s → нет агро: %s",
                    date_key,
                    ", ".join(map(str, missing))
                )

            filtered_pairs.append((date_key, prefix, ula, ulb))

        # ---------------------------
        # Итог логирования
        # ---------------------------
        if not filtered_pairs:
            self.logger.info("Нет данных для обработки")
            return

        self.logger.info(
            "К ОБРАБОТКЕ (%d дат): %s",
            len(filtered_pairs),
            ", ".join([p[0] for p in filtered_pairs])
        )

        # ---------------------------
        # Обработка
        # ---------------------------
        for idx, (date_key, tile_base, zip_ula, zip_ulb) in enumerate(
                filtered_pairs):
            self.logger.info(
                "[%d/%d] Обработка даты: %s",
                idx + 1,
                len(filtered_pairs),
                date_key
            )

            try:
                ula_ok, ula_reason = self._process_zip(zip_ula)
                ulb_ok, ulb_reason = self._process_zip(zip_ulb)

                if not ula_ok or not ulb_ok:
                    self.logger.warning(
                        "Ошибка пары %s | ULA=%s | ULB=%s",
                        date_key,
                        ula_reason or "OK",
                        ulb_reason or "OK",
                    )
                    if not debug:
                        self._clean()
                    continue

                execute_publisher()

                if not debug:
                    self._clean()

                self.logger.info("SUCCESS %s", date_key)

            except Exception as exc:
                self.logger.exception("FATAL %s: %s", date_key, exc)
                return

        self.logger.info("Обработка завершена")

    def _run_step(
        self,
        step_name: str,
        func: Callable[..., Any],
        **kwargs
    ) -> Tuple[bool, Optional[str]]:
        """
        Запускает один шаг пайплайна и возвращает:
        - True, None если всё прошло успешно
        - False, строку с причиной ошибки
        """
        try:
            self.logger.info("STEP START: %s", step_name)
            result = func(**kwargs)

            # Если функция явно вернула False — считаем это ошибкой.
            # Если вернула None/True — считаем успехом.
            if result is False:
                reason = f"{step_name} вернул False"
                self.logger.warning(reason)
                return False, reason

            self.logger.info("STEP OK: %s", step_name)
            return True, None

        except Exception as exc:
            self.logger.exception("STEP FAIL: %s | %s", step_name, exc)
            return False, f"{step_name}: {exc}"

    def _process_zip(self, zip_file: str) -> Tuple[bool, Optional[str]]:
        """
        Полный цикл обработки одного zip-файла.

        Возвращает:
        - (True, None) при успехе
        - (False, reason) при ошибке
        """
        self.logger.info("ZIP: %s", zip_file)

        zip_obj = ZipHandler(zip_file)
        info = zip_obj.get_zip_info()

        level = info.level.lower()
        tile = info.tile.lower()
        date = info.date.strftime("%d_%m_%Y")
        satellite = info.satellite.lower()

        agroids = [1, 3, 4] if tile == "t38ula" else [1, 5, 6]

        kwargs = {
            "date": date,
            "tile": tile,
            "satellite": satellite,
            "level": level,
        }

        needed_files = ["TCI", "SCL", "B03", "B04", "B08"]
        levels = {"msil1c": "L1C", "msil2a": "L2A"}

        self.logger.info(
            "ZIP info: basename=%s | tile=%s | level=%s | date=%s | satellite=%s",
            zip_obj.basename,
            tile,
            level,
            date,
            satellite,
        )

        try:
            self.logger.info("Распаковка ZIP: %s", zip_file)
            zip_file_path = zip_obj.unzip(
                dst_path=settings.TEMP_PROCESSING_DIR,
                needed_files=needed_files,
                level=levels[level],
            )
        except Exception as exc:
            self.logger.exception("UNZIP ERROR %s: %s", zip_file, exc)
            return False, f"unzip: {exc}"

        if not os.path.exists(zip_file_path):
            reason = f"после распаковки не найден путь: {zip_file_path}"
            self.logger.warning(reason)
            return False, reason

        steps = [
            ("tile_image_processor", execute_tile_image_processor, kwargs),
            (
                "sentinel_image_processor",
                execute_sentinel_image_processor,
                {**kwargs, "agroids": agroids},
            ),
            ("combine_image_processor", execute_combine_image_processor, kwargs),
            (
                "cloud_mask_image_processor",
                execute_cloud_mask_image_processor,
                {**kwargs, "agroids": agroids},
            ),
            ("ndvi_statistics_image_processor", execute_ndvi_statistics_image_processor, kwargs),
        ]

        for step_name, step_func, step_kwargs in steps:
            ok, reason = self._run_step(step_name, step_func, **step_kwargs)
            if not ok:
                return False, reason

        return True, None

    @staticmethod
    def _clean():
        remove_files_from_dir(
            settings.INTERMEDIATE,
            settings.PROCESSED_DIR,
            settings.NDVI_DIR,
            settings.TEMP_PROCESSING_DIR,
        )