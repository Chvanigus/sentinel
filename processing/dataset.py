"""Управление временем жизни GDAL dataset и запись массивов."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import numpy as np
from osgeo import gdal

gdal.UseExceptions()


@contextmanager
def open_raster(path: str | Path, mode=gdal.GA_ReadOnly):
    """Открывает GDAL dataset и освобождает ссылку после использования."""
    dataset = gdal.Open(str(path), mode)
    if dataset is None:
        raise FileNotFoundError(f"Не удалось открыть растр: {path}")
    try:
        yield dataset
    finally:
        dataset = None


class RasterArrayWriter:
    """Записывает 2D NumPy-массив с геопривязкой исходного растра."""

    def __init__(
            self,
            source: str | Path,
            destination: str | Path,
            nodata: float | None,
            output_format: str = "GTiff",
    ):
        self.source = Path(source)
        self.destination = Path(destination)
        self.nodata = nodata
        self.output_format = output_format

    def write(self, array: np.ndarray) -> None:
        """Записывает двумерный массив в новый одноканальный растр."""
        if array.ndim != 2:
            raise ValueError("RasterArrayWriter принимает только 2D-массив")

        with open_raster(self.source) as source:
            driver = gdal.GetDriverByName(self.output_format)
            output = driver.Create(
                str(self.destination),
                array.shape[1],
                array.shape[0],
                1,
                gdal.GDT_Float32,
            )
            if output is None:
                raise RuntimeError(
                    f"Не удалось создать растр: {self.destination}"
                )
            output.SetGeoTransform(source.GetGeoTransform())
            output.SetProjection(source.GetProjection())
            band = output.GetRasterBand(1)
            band.WriteArray(array)
            if self.nodata is not None:
                band.SetNoDataValue(self.nodata)
            band.FlushCache()
            band = None
            output.FlushCache()
            output = None
