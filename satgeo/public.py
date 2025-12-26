"""
Публикация снимков с processed → GeoServer → архив.
"""
import datetime
import os
from os import walk
from os.path import join

import psycopg2

from core import settings
from core.filesystem import LocalFS, RemoteFS
from core.logging import get_logger
from core.utils import split_file_name, get_basename
from db.connect_data import DSL
from db.data_class import Layer
from db.db_class import get_postgis_worker
from satgeo.client import GeoServerClient


def _ensure_remote_dirs(path: str):
    """
    Убеждается, что на удалённом сервере существует директория.
    Пока placeholder: можно расширить через SSH.
    """
    # TODO: реализовать проверку и создание через SSH
    return


class Publisher:
    """
    Класс для публикации спутниковых снимков из папки processed.

    Алгоритм:
      1. Копирование снимка на удалённый GeoServer (SFTP).
      2. Создание/обновление store и coverage через REST API.
      3. Кеширование через GWC при необходимости.
      4. Запись метаданных в PostGIS.
      5. Копирование снимка в архив.
      6. Удаление исходного файла из processed.
    """

    def __init__(self):
        """
        Инициализация:
          - логгер из core.logging.get_logger;
          - локальный и удалённый файловый менеджеры;
          - клиент GeoServer;
          - директория архива.
        """
        self.logger = get_logger()
        self.local_fs = LocalFS()
        self.remote_fs = RemoteFS()
        self.gs_client = GeoServerClient()
        self.archive_root = settings.ARCHIVE_DIR

    def publish_all(self):
        """
        Публикует все TIFF-файлы из папки processed.
        :return: None
        """
        self.logger.info("Начало процесса публикации снимков.")
        self.gs_client.ensure_workspace()

        for root, _, files in walk(settings.PROCESSED_DIR):
            for fname in files:
                if fname.startswith("__group__"):
                    self.logger.info(f"Пропускаем служебный файл: {fname}")
                    continue

                local_path = join(root, fname)
                self.logger.info(f"Обработка файла: {local_path}")

                name = get_basename(local_path).rsplit(".", 1)[0]

                # 1. Разбор даты из имени
                try:
                    dt_str, img_set, res, agroid, _, lname, sat = split_file_name(
                        name)
                    date = datetime.datetime.strptime(dt_str,
                                                      "%d_%m_%Y").date()
                except Exception as e:
                    self.logger.error(f"Неправильное имя файла '{fname}': {e}")
                    continue

                year = date.year
                remote_dir = f"/opt/geoware/SENTINEL{year}"
                remote_path = f"{remote_dir}/{name}.tif"

                # 2. Копируем на GeoServer
                self.logger.info(
                    f"Копирование '{fname}' на GeoServer: {remote_path}")
                try:
                    _ensure_remote_dirs(remote_dir)
                    self.remote_fs.copy(
                        src=local_path,
                        host=settings.RMHOST,
                        port=settings.SSH_PORT,
                        user=settings.RMUSER,
                        pwd=settings.RMPASSWORD,
                        dst=remote_path
                    )
                    self.logger.info(f"SFTP копирование успешно: {fname}")
                except Exception as e:
                    self.logger.error(f"Ошибка SFTP для {fname}: {e}")
                    continue

                # 3. REST: store + coverage
                self.logger.info(
                    f"Обновление GeoServer store и coverage для: {name}"
                )
                deleted = self.gs_client.delete_store(name)
                if not deleted:
                    self.logger.info(
                        f"Store '{name}' не существовал, будет создан."
                    )
                store = self.gs_client.create_store(name, remote_path)
                layer = self.gs_client.create_coverage(store, name)

                # 4. GWC
                if layer and settings.USE_GWC:
                    self.logger.info(f"Запуск GWC reseed для слоя: {name}")
                    self.gs_client.seed_gwc(name, settings.ZOOM_START,
                                            settings.ZOOM_STOP)

                # 5. PostGIS
                if layer:
                    self.logger.info(f"Запись метаданных в базу для: {name}")
                    try:
                        with psycopg2.connect(**DSL) as conn:
                            pw = get_postgis_worker(conn)
                            pw.insert_layer(
                                Layer(
                                    name=f"{settings.WORKSPACE}:{lname}",
                                    date=date,
                                    set=img_set,
                                    resolution=res,
                                    agroid=agroid,
                                    satellite=sat
                                )
                            )
                        self.logger.info(f"Запись в PostGIS успешна: {name}")
                    except Exception as e:
                        self.logger.error(
                            f"Ошибка записи в PostGIS для {name}: {e}")

                # 6. Архивирование и удаление
                archive_dir = os.path.join(f"{self.archive_root}{year}")
                self.logger.info(
                    f"Копирование '{fname}' в архив: {archive_dir}")
                try:
                    self.local_fs.copy(local_path, archive_dir)
                    self.logger.info(f"Архивирование успешно: {fname}")
                except Exception as e:
                    self.logger.error(f"Ошибка при архивировании {fname}: {e}")

                self.logger.info("=" * 50)

        self.logger.info("Публикация всех снимков завершена.")


def execute_publisher():
    """Вызов класса Publisher."""
    pub = Publisher()
    pub.publish_all()
