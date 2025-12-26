"""Класс для сбора статистики NDVI по спутниковым снимкам."""
import os
from datetime import datetime
from typing import List, Optional

import cv2
import numpy as np
import psycopg2
from osgeo import gdal
from scipy.ndimage import binary_erosion

from core import settings
from core.utils import get_date_obj
from db.connect_data import DSL
from db.data_class import Field, NdviValues
from db.db_class import get_postgis_worker
from processing.processors.base import BaseImageProcessor, BasePathManager
from processing.rastr import RastrProcessing


class NdviFieldAnalyzer:
    """
    Класс предназначен для анализа значений NDVI по полю.
    Выполняет расчёт базовой статистики
    (среднее, минимум, максимум, стандартное отклонение),
    коэффициент вариации и определение однородности растительности по полю.
    """

    def __init__(self, nodata_value: float = -9999.0):
        self.nodata_value = nodata_value

    def analyze_ndvi_array(
            self,
            ndvi_arr: np.ndarray,
            date_obj,
            field_id: int,
            ndvi_path: str = None,
            agroid: int = None,
            fieldcode: str = None,
            save_canny: bool = False
    ) -> NdviValues or None:
        """
        Выполняет анализ массива NDVI по одному полю.

        Вычисляет:
        - Среднее, минимальное, максимальное и стандартное отклонение NDVI.
        - Коэффициент вариации NDVI.
        - Оценку однородности поля.
        - Сохраняет валидные значения NDVI в байтовом представлении.

        Параметры:
        ----------
        ndvi_arr : np.ndarray
            Массив NDVI значений поля (2D).
        date_obj : datetime.date
            Дата снимка.
        field_id : int
            Идентификатор поля.

        Возвращает:
        -----------
        NdviValues или None
            Объект со статистикой NDVI или None, если нет валидных данных.
        """
        try:
            valid_mask = (ndvi_arr != self.nodata_value) & ~np.isnan(ndvi_arr)
        except TypeError:
            valid_mask = None
        if not np.any(valid_mask):
            return None

        valid_ndvi = ndvi_arr[valid_mask]

        mean_ndvi = float(np.nanmean(valid_ndvi))
        max_ndvi = float(np.nanmax(valid_ndvi))
        min_ndvi = float(np.nanmin(valid_ndvi))
        std_ndvi = float(np.nanstd(valid_ndvi))

        ndvi_cv = (std_ndvi / mean_ndvi) * 100 if mean_ndvi else 0.0
        is_uniform = self.is_uniform(
            ndvi_arr, ndvi_path, agroid, date_obj, fieldcode, True
        )

        return NdviValues(
            date=date_obj,
            fieldid=field_id,
            ndvimean=mean_ndvi,
            ndvimax=max_ndvi,
            ndvimin=min_ndvi,
            growth_percent=0.0,
            ndvi_cv=ndvi_cv,
            is_uniform=is_uniform,
        )

    @staticmethod
    def is_uniform(ndvi_arr: np.ndarray,
                   ndvi_path: str = None,
                   agroid: int = None,
                   date_obj: datetime.date = None,
                   fieldcode: str = None,
                   save: bool = False) -> bool:
        """
        Оценивает однородность поля по значению NDVI на основе двух критериев:
        статистической однородности и текстурной (градиентной) гладкости.

        - Использует коэффициент вариации и медианное абсолютное отклонение.
        - Выполняет фильтрацию краёв через алгоритм Canny (OpenCV)
        после гауссового размытия.

        Параметры:
        ----------
        ndvi_arr : np.ndarray
            Массив NDVI значений поля.

        Возвращает:
        -----------
        bool
            True, если поле считается однородным, иначе False.
        """
        valid_mask = (ndvi_arr > 0) & ~np.isnan(ndvi_arr)
        if not np.any(valid_mask):
            return False

        eroded_mask = binary_erosion(valid_mask, structure=np.ones((5, 5)))
        ndvi_valid = ndvi_arr[eroded_mask]

        if ndvi_valid.size == 0:
            return False

        mean_ndvi = np.mean(ndvi_valid)
        if mean_ndvi < 0.15:
            return False

        ndvi_cv = (np.std(ndvi_valid) / mean_ndvi) * 100
        mad = np.median(np.abs(ndvi_valid - np.median(ndvi_valid)))

        # === OpenCV подход ===
        filled_ndvi = np.copy(ndvi_arr)
        filled_ndvi[~valid_mask] = mean_ndvi
        filled_ndvi = cv2.GaussianBlur(filled_ndvi, (5, 5), sigmaX=1.0)

        # Используем Canny из OpenCV
        ndvi_uint8 = np.clip(filled_ndvi * 255, 0, 255).astype(np.uint8)
        edges = cv2.Canny(ndvi_uint8, threshold1=20, threshold2=60)

        if save and ndvi_path and agroid is not None and fieldcode is not None and date_obj:
            NdviFieldAnalyzer.save_canny_edges(edges, ndvi_path, agroid,
                                               date_obj, fieldcode)

        edge_ratio = np.sum(edges > 0) / edges.size

        is_uniform_stat = ndvi_cv < 20 and mad < 0.03
        is_uniform_edge = edge_ratio < 0.02

        return bool(is_uniform_stat and is_uniform_edge)

    @staticmethod
    def save_canny_edges(edges: np.ndarray,
                         ndvi_path: str,
                         agroid: int,
                         date_obj: datetime.date,
                         fieldcode: str) -> None:
        """
        Сохраняет Canny-маску в GeoTIFF с геопривязкой из NDVI файла.
        """

        # Получаем привязку и размер с NDVI исходника
        ds = gdal.Open(ndvi_path)
        geo_transform = ds.GetGeoTransform()
        projection = ds.GetProjection()
        driver = gdal.GetDriverByName("GTiff")

        save_path = os.path.join(
            settings.NDVI_DIR,
            f"A{agroid}_{date_obj.strftime('%d_%m_%Y')}_FIELD{fieldcode}_canny.tif"
        )

        out_ds = driver.Create(save_path, edges.shape[1], edges.shape[0], 1,
                               gdal.GDT_Byte)
        out_ds.SetGeoTransform(geo_transform)
        out_ds.SetProjection(projection)
        out_ds.GetRasterBand(1).WriteArray(edges)
        out_ds.FlushCache()
        out_ds = None


class NdviStatisticsImageProcessor(BaseImageProcessor):
    """Класс для сбора и анализа статистики NDVI по спутниковым снимкам."""

    def __init__(self, tile, date, satellite, path_manager) -> None:
        super().__init__(tile, date, satellite, path_manager)
        self.agroids = [1, 3, 4, 5, 6]
        self.date_obj = get_date_obj(date)
        self.analyzer = NdviFieldAnalyzer(nodata_value=-9999.0)

    @staticmethod
    def _get_fields_list(agroid: int) -> list:
        with psycopg2.connect(**DSL) as pg_conn:
            pw = get_postgis_worker(pg_conn)
            fields = pw.get_fieldids_from_agro(
                agroid=agroid, year=settings.YEAR
            )
        return fields

    def _save_field_geojson(self, field: Field, agroid: int) -> None:
        with psycopg2.connect(**DSL) as pg_conn:
            pw = get_postgis_worker(pg_conn)
            pw.save_field_geojson(
                fieldid=field.id, fieldname=field.name,
                agroid=agroid, year=settings.YEAR,
                dst_path=settings.NDVI_DIR, date=self.date
            )

    @staticmethod
    def _save_ndvi_values_for_agro(ndvi_values: List[NdviValues]) -> None:
        with psycopg2.connect(**DSL) as pg_conn:
            pw = get_postgis_worker(pg_conn)
            pw.insert_ndvi_data(ndvi_values=ndvi_values)

    def _process_files(self):
        # 1) Для каждого агро: берём фильтрованный NDVI, список полей, анализируем
        for agroid in self.agroids:
            src_ndvi = self.pm.get_sources(stage="ndvi", agroid=agroid)[0]
            self.logger.info(f"Агро {agroid}: проверка {src_ndvi}")
            if not os.path.exists(src_ndvi):
                self.logger.warning(f"NDVI не найден → пропуск: {src_ndvi}")
                continue

            fields = self._get_fields_list(agroid)
            ndvi_values = []

            for field in fields:
                geojson = self.pm.field_geojson(agroid, field.name)
                # генерируем geojson, если нет
                if not os.path.exists(geojson):
                    self._save_field_geojson(field, agroid)

                if not os.path.exists(geojson):
                    self.logger.warning(f"GeoJSON не найден → {geojson}")
                    continue

                dst_tif = self.pm.field_ndvi_tif(agroid, field.name)
                if not os.path.exists(dst_tif):
                    RastrProcessing(
                        src_path=src_ndvi, dst_path=dst_tif
                    ).clip_by_shp(
                        mask_file_path=geojson, x_res=10, y_res=10
                    )

                val = self.analyzer.analyze_ndvi_array(
                    ndvi_arr=self._load_ndvi_array(dst_tif),
                    date_obj=self.date_obj,
                    field_id=field.id,
                    ndvi_path=dst_tif,
                    agroid=agroid,
                    fieldcode=field.name,
                    save_canny=True
                )
                if val:
                    ndvi_values.append(val)

            if ndvi_values:
                self.logger.info(
                    f"Агро {agroid}: сохраняем {len(ndvi_values)} записей в БД")
                self._save_ndvi_values_for_agro(ndvi_values)

    @staticmethod
    def _load_ndvi_array(path: str) -> Optional[np.ndarray]:
        if not os.path.exists(path):
            return None
        ds = gdal.Open(path)
        return ds.ReadAsArray().astype(np.float32) if ds else None


class NdviStatsPathManager(BasePathManager):
    """
    Отвечает за пути для:
      - отфильтрованного NDVI (input)
      - GeoJSON полей (input/output)
      - вырезанных TIFF по полю (intermediate)
    """

    def get_destination(self, *, stage: str,
                        agroid: Optional[int] = None) -> str:
        """В данном случае у нас нет выходного файла."""
        pass

    def get_sources(self, *, stage: str, agroid: Optional[int] = None) -> List[
        str]:
        return [
            os.path.join(
                settings.INTERMEDIATE,
                f"{self.satellite}_{self.date}_a{agroid}_ndvi_10m_3857_filtered.tif")
        ]

    def field_geojson(self, agroid: int, fieldcode: str) -> str:
        """Возвращает название файла поля в формате geojson."""
        return os.path.join(
            settings.NDVI_DIR,
            f"A{agroid}_{self.date}_FIELD{fieldcode}.geojson"
        )

    def field_ndvi_tif(self, agroid: int, fieldcode: str) -> str:
        """Возвращает название файла NDVI поля в формате .tif."""
        return os.path.join(
            settings.NDVI_DIR,
            f"A{agroid}_{self.date}_FIELD{fieldcode}_ndvi.tif"
        )


def execute_ndvi_statistics_image_processor(**kwargs) -> None:
    """
    Вызов класса для сбора статистики NDVI по агропредприятиям.
    :param kwargs: Дополнительные параметры для обработки снимков.
    """
    pm = NdviStatsPathManager(**kwargs)
    processor = NdviStatisticsImageProcessor(**kwargs, path_manager=pm)
    processor.execute()
