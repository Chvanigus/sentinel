"""Атомарные операции преобразования и вырезки растров GDAL."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
from osgeo import gdal

from processing.dataset import atomic_raster_path, open_raster

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
    """Массив вырезанного растра и точная маска пикселей полигона."""

    values: np.ndarray
    coverage: np.ndarray


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


def clip_by_mask_array(
        source: str | Path,
        mask: str | Path,
        *,
        x_resolution: float | None = None,
        y_resolution: float | None = None,
        nodata: float | None = None,
) -> np.ndarray:
    """Обрезает растр в памяти и возвращает массив без временного TIFF."""
    with open_raster(source) as dataset:
        x_resolution, y_resolution = _resolution(
            dataset,
            x_resolution,
            y_resolution,
        )
        result = gdal.Warp(
            "",
            dataset,
            format="MEM",
            cutlineDSName=str(mask),
            cropToCutline=True,
            xRes=x_resolution,
            yRes=y_resolution,
            dstSRS=dataset.GetProjection(),
            srcNodata=nodata,
            dstNodata=nodata,
            multithread=True,
            warpOptions=[
                *WARP_OPTIONS,
                "INIT_DEST=NO_DATA",
            ],
        )
        if result is None:
            raise RuntimeError(f"GDAL не смог обрезать изображение {source}")
        try:
            array = result.ReadAsArray()
            if array is None:
                raise RuntimeError(
                    f"GDAL не смог прочитать вырезанный растр {source}"
                )
            return np.asarray(array, dtype=np.float32)
        finally:
            result = None


def clip_by_mask_with_coverage(
        source: str | Path,
        mask: str | Path,
        *,
        x_resolution: float | None = None,
        y_resolution: float | None = None,
        nodata: float | None = None,
) -> RasterClip:
    """Вырезает растр и отдельно растеризует точную область поля."""
    with open_raster(source) as dataset:
        x_resolution, y_resolution = _resolution(
            dataset,
            x_resolution,
            y_resolution,
        )
        result = gdal.Warp(
            "",
            dataset,
            format="MEM",
            cutlineDSName=str(mask),
            cropToCutline=True,
            xRes=x_resolution,
            yRes=y_resolution,
            dstSRS=dataset.GetProjection(),
            srcNodata=nodata,
            dstNodata=nodata,
            multithread=True,
            warpOptions=[
                *WARP_OPTIONS,
                "INIT_DEST=NO_DATA",
            ],
        )
        if result is None:
            raise RuntimeError(f"GDAL не смог обрезать изображение {source}")

        vector = None
        coverage_dataset = None
        try:
            values = result.ReadAsArray()
            if values is None:
                raise RuntimeError(
                    f"GDAL не смог прочитать вырезанный растр {source}"
                )

            vector = gdal.OpenEx(str(mask), gdal.OF_VECTOR)
            if vector is None:
                raise RuntimeError(f"GDAL не смог открыть маску {mask}")
            layer = vector.GetLayer()
            driver = gdal.GetDriverByName("MEM")
            coverage_dataset = driver.Create(
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
            return RasterClip(
                values=np.asarray(values, dtype=np.float32),
                coverage=np.asarray(coverage, dtype=bool),
            )
        finally:
            coverage_dataset = None
            vector = None
            result = None
