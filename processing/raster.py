"""Атомарные операции преобразования и вырезки растров GDAL."""
from __future__ import annotations

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
