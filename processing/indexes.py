"""Класс для работы с индексами спутниковых снимков."""
from typing import Tuple, Union, Any, Optional, Callable

import numpy as np
from osgeo import gdal

from processing.dataset import GDALDatasetProcessing, GDALDatasetContextManager

np.seterr(divide='ignore', invalid='ignore')


class IndexProcessing:
    """Класс для создания пространственных индексов."""

    def __init__(
        self,
        output_file: str,
        b03_file: Optional[str] = None,
        b04_file: Optional[str] = None,
        b08_file: Optional[str] = None,
    ):
        self._b03_file = b03_file
        self._b04_file = b04_file
        self._b08_file = b08_file
        self._output_file = output_file

    @staticmethod
    def _load_band(path: str) -> np.ndarray:
        """Возвращает спектральное изображение как массив NumPy."""
        with GDALDatasetContextManager(path) as ds:
            return ds.ReadAsArray().astype(np.float64)

    @staticmethod
    def _calculate_index(
            band_a: np.ndarray, band_b: np.ndarray,
            formula: Callable[[np.ndarray, np.ndarray], np.ndarray]
    ) -> np.ndarray:
        """Общий метод вычисления индекса."""
        result = formula(band_a, band_b)
        return np.clip(result, -1.0, 1.0)

    def _create_index_image(
            self,
            band_a_file: str,
            band_b_file: str,
            formula: Callable[[np.ndarray, np.ndarray], np.ndarray]
    ) -> None:
        """Создаёт изображение из двух спектральных изображений."""
        band_a = self._load_band(band_a_file)
        band_b = self._load_band(band_b_file)

        index_array = self._calculate_index(band_a, band_b, formula)

        GDALDatasetProcessing(
            dst_path=self._output_file,
            src_path=band_a_file,
            np_array=index_array
        ).create_file_from_array()

    def create_ndvi_image(self) -> None:
        """Создание NDVI (Normalized Difference Vegetation Index)."""
        if not self._b04_file or not self._b08_file:
            raise ValueError("Не заданы пути к B04 и B08 для NDVI.")
        self._create_index_image(
            self._b08_file,
            self._b04_file,
            lambda nir, red: (nir - red) / (nir + red)
        )

    def create_ndwi_image(self) -> None:
        """Создание NDWI (Normalized Difference Water Index)."""
        if not self._b03_file or not self._b08_file:
            raise ValueError("Не заданы пути к B03 и B08 для NDWI.")
        self._create_index_image(
            self._b03_file,
            self._b08_file,
            lambda green, nir: (green - nir) / (green + nir)
        )