"""Пересечение границ хозяйства с экстентом растра."""

from __future__ import annotations

from pathlib import Path

from osgeo import osr

from processing.dataset import open_raster


def intersect_raster_bounds(
        bounds: tuple[float, float, float, float],
        source: str | Path,
        destination_srid: int,
) -> tuple[float, float, float, float]:
    """Возвращает пересечение bounds с растром в целевой системе координат."""
    with open_raster(source) as dataset:
        source_srs = osr.SpatialReference(wkt=dataset.GetProjection())
        destination_srs = osr.SpatialReference()
        destination_srs.ImportFromEPSG(destination_srid)
        transformer = osr.CoordinateTransformation(
            source_srs,
            destination_srs,
        )
        geotransform = dataset.GetGeoTransform()
        width = dataset.RasterXSize
        height = dataset.RasterYSize
        pixel_corners = (
            (0, 0),
            (width, 0),
            (width, height),
            (0, height),
        )
        raster_corners = [
            (
                geotransform[0]
                + x * geotransform[1]
                + y * geotransform[2],
                geotransform[3]
                + x * geotransform[4]
                + y * geotransform[5],
            )
            for x, y in pixel_corners
        ]
        transformed = transformer.TransformPoints(raster_corners)

    x_coordinates = [point[0] for point in transformed]
    y_coordinates = [point[1] for point in transformed]
    return (
        max(bounds[0], min(x_coordinates)),
        max(bounds[1], min(y_coordinates)),
        min(bounds[2], max(x_coordinates)),
        min(bounds[3], max(y_coordinates)),
    )
