"""Класс для публикации снимков на Geoserver."""
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2

from core import settings
from core.logging import get_logger
from db.connect_data import DSL
from db.data_class import Layer
from db.db_class import get_postgis_worker
from satgeo.client import GeoServerClient
from satgeo.utils import split_file_name, optimize_geotiff


class GeoServerPublic:
    """
    Утилита высокого уровня для публикации/удаления растров в GeoServer.
    """
    STYLE_MAP = {
        "ndvi": "ndvi",
        "ndwi": "ndwi",
        "scl": "scl",
        "tci": "tci"
    }

    def __init__(self):
        self.client = GeoServerClient()
        self.logger = get_logger("GeoserverPublic")

    @staticmethod
    def _build_root_layer_path(date: datetime.date,
                               agroid: str,
                               img_type: str) -> Path:
        """Возвращает путь к слою в хранилище."""
        root = Path(settings.GS_DATA_ROOT)
        return (
                root /
                str(date.year) /
                f"a{agroid}" /
                img_type /
                str(date.month) /
                f"a{agroid}_{img_type}_{date.isoformat()}.tif"
        )

    @staticmethod
    def _build_store_name(img_type: str, agroid: Optional[str],
                          date: str) -> str:
        """
        Возвращает имя coveragestore / слоя: a{agroid}_{img_type}_{date}.
        """
        return f"a{agroid}_{img_type}_{date}"

    @staticmethod
    def _to_container_path(host_path: Path) -> str:
        """
        Преобразовать путь на хосте в путь внутри контейнера GeoServer.
        """
        return str(
            Path(host_path).as_posix()
        ).replace(settings.GS_DATA_ROOT, settings.GS_DATA_DIR)

    def _optimize_geotiff_file_to_root(self, src: Path, dst: Path):
        """Оптимизация GeoTIFF файла в корневую директорию Geoserver."""
        if not dst.exists():
            self.logger.info(
                "Оптимизация GeoTIFF файла %s в %s", src, dst
            )
            optimize_geotiff(src=src, dst=dst)
        else:
            self.logger.info("Файл %s уже существует, без оптимизации", dst)

    def _publish_file(self, file_path: Path) -> (bool, str):
        """
        Публикация файла в Geoserver.
        Возвращает кортеж (успех, имя слоя).
        """
        info = split_file_name(file_path.name)

        img_type = info.img_type
        style = self.STYLE_MAP.get(img_type)
        agroid = info.agroid
        date = info.date()

        layer_name = f"a{agroid}_{img_type}_{date.isoformat()}"
        store_name = f"{layer_name}_store"

        self.logger.info(
            "Публикация %s -> layer=%s",
            file_path.name, layer_name
        )

        # 1. Куда кладём оптимизированный файл
        dst = self._build_root_layer_path(date, agroid, img_type)

        try:
            self._optimize_geotiff_file_to_root(
                src=file_path,
                dst=dst,
            )
        except Exception as exc:
            self.logger.error(
                "Пропуск файла %s: ошибка оптимизации GeoTIFF: %s",
                file_path, exc,
                exc_info=True,
            )
            return False, f"optimize_failed: {file_path.name}"

        # 2. Store / Coverage
        container_path = self._to_container_path(dst)

        store = self.client.get_or_create_store(
            store_name=store_name,
            container_path=container_path,
        )

        if store:
            self.logger.info("Создан store для слоя %s", layer_name)

        # 3. Стиль
        if style and style != "tci":
            self.client.set_layer_style(
                layer_name=layer_name,
                style_name=style,
            )
            self.logger.info(
                f"Назначен стиль '%s' для слоя '%s'",
                style, layer_name
            )

        self.logger.info(
            f"Снимок %s добавлен на GeoServer, "
            f"добавляем в базу данных", layer_name
        )

        # Добавление в базу данных
        self._make_row_in_db(file_path, layer_name)

        self.logger.info(
            "Установка кэша для %s", layer_name
        )

        # Установка кэша
        self.client.enable_gwc_gridset_3857(layer_name)

        if settings.YEAR == date.year:
            # Прогрев кэша с использованием bbox
            with psycopg2.connect(**DSL) as conn:
                pw = get_postgis_worker(conn)
                bbox = pw.get_bounds_lats_lons(
                    year=date.year,
                    agroid=int(agroid),
                    dstype=3857,
                )

            self.client.seed_gwc_cache(
                layer_name=layer_name,
                zoom_start=8,
                zoom_stop=14,
                threads=4,
                image_format="image/png",
                bbox=bbox,
            )

        return True, layer_name

    def _make_row_in_db(self, file_path: Path, layer: str) -> None:
        """Добавление строки в базу данных."""
        info = split_file_name(file_path.name)
        date = info.date()
        img_set = info.img_type

        agroid = info.agroid
        if 'a' in agroid.lower():
            agroid = agroid[1:]

        with psycopg2.connect(**DSL) as conn:
            pw = get_postgis_worker(conn)
            pw.insert_layer(
                Layer(
                    name=f"{settings.GS_WORKSPACE}:{layer}",
                    date=date,
                    set=img_set,
                    agroid=int(agroid),
                )
            )
        self.logger.info(f"Запись в PostGIS успешна: {layer}")

    def publish_all(self):
        """Публикация всех файлов в директории готовых снимков."""
        self.logger.info("Начало процесса публикации снимков")
        for root, _, files in os.walk(settings.PROCESSED_DIR):
            for fname in files:
                if not fname.endswith(".tif"):
                    self.logger.info(
                        "Пропускаем служебный файл: %", fname
                    )

                    continue

                file_path = Path(root) / fname
                self.logger.info("Обрабатываем: %s", fname)

                self._publish_file(file_path)

                self.logger.info("Файл опубликован штатно: %s", fname)

        self.logger.info("Процесс публикации завершён")


def execute_publisher() -> None:
    """Вызов класса GeoServerPublic."""
    pub = GeoServerPublic()
    pub.publish_all()
