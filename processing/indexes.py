"""Класс для работы с индексами спутниковых снимков."""
from __future__ import annotations

from contextlib import ExitStack
from time import perf_counter

import numpy as np

from core.logging import get_logger
from processing.calculations import normalized_difference
from processing.dataset import (
    create_raster_like,
    ensure_same_grid,
    iter_raster_windows,
    open_raster,
)


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
        self.logger = get_logger(self.__class__.__name__)

    @staticmethod
    def _read_window(dataset, window) -> np.ndarray:
        """Читает одно окно первого канала как ``float32``."""
        array = dataset.GetRasterBand(1).ReadAsArray(*window)
        if array is None:
            raise RuntimeError(f"GDAL не смог прочитать окно {window}")
        return np.asarray(array, dtype=np.float32)

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

        if "ndvi" in outputs and self._b04_file is None:
            raise ValueError("Не задан канал B04 для NDVI")
        if "ndwi" in outputs and self._b03_file is None:
            raise ValueError("Не задан канал B03 для NDWI")

        started = perf_counter()
        block_count = 0
        with ExitStack() as stack:
            b08 = stack.enter_context(open_raster(self._b08_file))
            sources = {}
            if "ndvi" in outputs:
                sources["ndvi"] = stack.enter_context(
                    open_raster(self._b04_file)
                )
            if "ndwi" in outputs:
                sources["ndwi"] = stack.enter_context(
                    open_raster(self._b03_file)
                )
            for product, dataset in sources.items():
                ensure_same_grid(b08, dataset, product.upper())

            destinations = {
                product: stack.enter_context(
                    create_raster_like(
                        b08,
                        destination,
                        nodata=self._nodata,
                    )
                )
                for product, destination in outputs.items()
            }

            for window in iter_raster_windows(b08):
                block_count += 1
                b08_array = self._read_window(b08, window)
                if "ndvi" in destinations:
                    b04_array = self._read_window(sources["ndvi"], window)
                    ndvi = normalized_difference(
                        b08_array,
                        b04_array,
                        nodata=self._nodata,
                    )
                    destinations["ndvi"].GetRasterBand(1).WriteArray(
                        ndvi,
                        window[0],
                        window[1],
                    )
                if "ndwi" in destinations:
                    b03_array = self._read_window(sources["ndwi"], window)
                    ndwi = normalized_difference(
                        b03_array,
                        b08_array,
                        nodata=self._nodata,
                    )
                    destinations["ndwi"].GetRasterBand(1).WriteArray(
                        ndwi,
                        window[0],
                        window[1],
                    )
        self.logger.info(
            "INDEX OK: продукты=%s блоков=%d | %.2f сек.",
            ", ".join(product.upper() for product in outputs),
            block_count,
            perf_counter() - started,
        )
