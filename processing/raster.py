"""Атомарные операции преобразования и вырезки растров GDAL."""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from osgeo import gdal

from processing.dataset import (
    atomic_raster_path,
    ensure_same_grid,
    open_raster,
)

TRANSLATE_OPTIONS = (
    "TILED=YES",
    "COMPRESS=DEFLATE",
    "PREDICTOR=2",
    "BIGTIFF=IF_SAFER",
    "NUM_THREADS=ALL_CPUS",
)
WARP_OPTIONS = ("CUTLINE_ALL_TOUCHED=TRUE", "NUM_THREADS=ALL_CPUS")


@dataclass(frozen=True)
class RasterClip:
    """NDVI, опциональная SCL и точная маска пикселей одного поля."""

    values: np.ndarray
    coverage: np.ndarray
    scl: np.ndarray | None = None


def _resolution(
        source,
        x_resolution: float | None,
        y_resolution: float | None,
) -> tuple[float, float]:
    """Возвращает явно заданное или исходное разрешение растра."""
    if x_resolution is None or y_resolution is None:
        transform = source.GetGeoTransform()
        x_resolution = abs(transform[1])
        y_resolution = abs(transform[5])
    return x_resolution, y_resolution


def translate_to_geotiff(
        source: str | Path,
        destination: str | Path,
) -> None:
    """Переводит растр в тайловый GeoTIFF, сохраняя исходный тип данных."""
    with open_raster(source) as dataset, atomic_raster_path(
            destination
    ) as temporary:
        result = gdal.Translate(
            destName=temporary,
            srcDS=dataset,
            options=gdal.TranslateOptions(
                format="GTiff",
                creationOptions=list(TRANSLATE_OPTIONS),
            ),
        )
        if result is None:
            raise RuntimeError(f"GDAL не смог конвертировать {source}")
        result.FlushCache()
        result = None


class FieldRasterReader:
    """Открывает растры хозяйства один раз и вырезает оба канала одним Warp."""

    def __init__(
            self,
            ndvi_path: str | Path,
            *,
            scl_path: str | Path | None = None,
            nodata: float = -9999.0,
    ) -> None:
        self.ndvi_path = Path(ndvi_path)
        self.scl_path = Path(scl_path) if scl_path is not None else None
        self.nodata = nodata
        self._stack: ExitStack | None = None
        self._source = None
        self._vrt = None
        self._vrt_path = f"/vsimem/field-reader-{id(self)}.vrt"

    def __enter__(self) -> FieldRasterReader:
        """Открывает NDVI/SCL и создаёт общий VRT на согласованной сетке."""
        self._stack = ExitStack()
        ndvi = self._stack.enter_context(open_raster(self.ndvi_path))
        self._source = ndvi
        if self.scl_path is not None:
            scl = self._stack.enter_context(open_raster(self.scl_path))
            ensure_same_grid(ndvi, scl, "SCL")
            self._vrt = gdal.BuildVRT(
                self._vrt_path,
                [ndvi, scl],
                separate=True,
            )
            if self._vrt is None:
                self.close()
                raise RuntimeError("GDAL не смог собрать общий NDVI/SCL VRT")
            self._source = self._vrt
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        """Освобождает VRT и исходные наборы данных хозяйства."""
        self.close()

    def close(self) -> None:
        """Идемпотентно закрывает все открытые GDAL-ресурсы."""
        has_vrt = self._vrt is not None
        self._source = None
        self._vrt = None
        if has_vrt:
            gdal.Unlink(self._vrt_path)
        if self._stack is not None:
            self._stack.close()
            self._stack = None

    def clip(self, mask: str | Path) -> RasterClip:
        """Вырезает NDVI/SCL поля одним Warp и строит точную coverage-маску."""
        if self._source is None:
            raise RuntimeError("FieldRasterReader должен быть открыт")
        x_resolution, y_resolution = _resolution(self._source, None, None)
        nodata = f"{self.nodata} 0" if self.scl_path is not None else self.nodata
        result = gdal.Warp(
            "",
            self._source,
            format="MEM",
            cutlineDSName=str(mask),
            cropToCutline=True,
            xRes=x_resolution,
            yRes=y_resolution,
            dstSRS=self._source.GetProjection(),
            srcNodata=nodata,
            dstNodata=nodata,
            multithread=True,
            warpOptions=[*WARP_OPTIONS, "INIT_DEST=NO_DATA"],
        )
        if result is None:
            raise RuntimeError(f"GDAL не смог обрезать изображение {mask}")
        try:
            arrays = result.ReadAsArray()
            if arrays is None:
                raise RuntimeError(
                    f"GDAL не смог прочитать вырезанный растр {mask}"
                )
            values, scl = self._split_arrays(arrays)
            return RasterClip(
                values=values,
                coverage=self._coverage(result, mask),
                scl=scl,
            )
        finally:
            result = None

    def _split_arrays(
            self,
            arrays: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray | None]:
        """Разделяет результат Warp на NDVI и опциональную SCL."""
        values = np.asarray(arrays, dtype=np.float32)
        if self.scl_path is None:
            return values, None
        if values.ndim != 3 or values.shape[0] != 2:
            raise RuntimeError(
                f"Ожидались два канала NDVI/SCL, получена форма {values.shape}"
            )
        return values[0], values[1]

    @staticmethod
    def _coverage(result, mask: str | Path) -> np.ndarray:
        """Растеризует точную область поля на сетку вырезанного NDVI."""
        vector = gdal.OpenEx(str(mask), gdal.OF_VECTOR)
        if vector is None:
            raise RuntimeError(f"GDAL не смог открыть маску {mask}")
        coverage_dataset = None
        try:
            layer = vector.GetLayer()
            coverage_dataset = gdal.GetDriverByName("MEM").Create(
                "",
                result.RasterXSize,
                result.RasterYSize,
                1,
                gdal.GDT_Byte,
            )
            coverage_dataset.SetGeoTransform(result.GetGeoTransform())
            coverage_dataset.SetProjection(result.GetProjection())
            coverage_band = coverage_dataset.GetRasterBand(1)
            coverage_band.Fill(0)
            error = gdal.RasterizeLayer(
                coverage_dataset,
                [1],
                layer,
                burn_values=[1],
                options=["ALL_TOUCHED=TRUE"],
            )
            if error != 0:
                raise RuntimeError(
                    f"GDAL не смог растеризовать маску {mask}"
                )
            coverage = coverage_band.ReadAsArray()
            if coverage is None:
                raise RuntimeError(
                    f"GDAL не смог прочитать растровую маску {mask}"
                )
            return np.asarray(coverage, dtype=bool)
        finally:
            coverage_dataset = None
            vector = None
