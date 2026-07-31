"""Класс для работы с индексами спутниковых снимков."""
from __future__ import annotations

import numpy as np

from processing.calculations import normalized_difference
from processing.dataset import RasterArrayWriter, open_raster


class SpectralIndexProcessor:
    """Класс для создания пространственных индексов."""

    def __init__(
            self,
            *,
            b03_file: str | None = None,
            b04_file: str | None = None,
            b08_file: str,
            nodata: float = -9999.0,
    ):
        self._b03_file = b03_file
        self._b04_file = b04_file
        self._b08_file = b08_file
        self._nodata = nodata

    @staticmethod
    def _load_band(path: str) -> np.ndarray:
        """Возвращает спектральное изображение как массив NumPy."""
        with open_raster(path) as ds:
            return ds.ReadAsArray().astype(np.float32)

    def _write_index(
            self,
            *,
            output_file: str,
            reference_file: str,
            primary: np.ndarray,
            secondary: np.ndarray,
    ) -> None:
        """Рассчитывает и записывает один спектральный индекс."""
        index_array = normalized_difference(
            primary,
            secondary,
            nodata=self._nodata,
        )
        RasterArrayWriter(
            destination=output_file,
            source=reference_file,
            nodata=self._nodata,
        ).write(index_array)

    def create(self, outputs: dict[str, str]) -> None:
        """Создаёт выбранные NDVI/NDWI, читая общий канал B08 один раз."""
        unknown = set(outputs) - {"ndvi", "ndwi"}
        if unknown:
            raise ValueError(
                "Неподдерживаемые спектральные индексы: "
                + ", ".join(sorted(unknown))
            )
        if not outputs:
            return

        b08 = self._load_band(self._b08_file)
        if "ndvi" in outputs:
            if self._b04_file is None:
                raise ValueError("Не задан канал B04 для NDVI")
            self._write_index(
                output_file=outputs["ndvi"],
                reference_file=self._b08_file,
                primary=b08,
                secondary=self._load_band(self._b04_file),
            )
        if "ndwi" in outputs:
            if self._b03_file is None:
                raise ValueError("Не задан канал B03 для NDWI")
            self._write_index(
                output_file=outputs["ndwi"],
                reference_file=self._b03_file,
                primary=self._load_band(self._b03_file),
                secondary=b08,
            )
