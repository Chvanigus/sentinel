"""Атомарные операции над растрами GDAL."""
from __future__ import annotations

from pathlib import Path

from osgeo import gdal


class RasterProcessor:
    """Translate и clip одного исходного растра."""

    def __init__(
            self,
            source: str | Path,
            destination: str | Path,
            output_format: str = "GTiff",
    ):
        self.source_path = Path(source)
        self.destination = Path(destination)
        self.output_format = output_format
        self.source = gdal.Open(str(self.source_path))
        if self.source is None:
            raise FileNotFoundError(
                f"Не удалось открыть растровый файл: {self.source_path}"
            )

    def clip_by_mask(
            self,
            mask: str | Path,
            x_resolution: float | None = None,
            y_resolution: float | None = None,
    ) -> None:
        """Обрезает растр по векторной маске с сохранением его проекции."""
        if x_resolution is None or y_resolution is None:
            transform = self.source.GetGeoTransform()
            x_resolution = abs(transform[1])
            y_resolution = abs(transform[5])

        self.destination.unlink(missing_ok=True)
        result = gdal.Warp(
            str(self.destination),
            self.source,
            format=self.output_format,
            cutlineDSName=str(mask),
            cropToCutline=True,
            xRes=x_resolution,
            yRes=y_resolution,
            dstSRS=self.source.GetProjection(),
            multithread=True,
            warpOptions=["CUTLINE_ALL_TOUCHED=TRUE"],
        )
        if result is None:
            raise RuntimeError(
                f"GDAL не смог обрезать изображение {self.source_path}"
            )
        result.FlushCache()
        result = None

    def translate_to_geotiff(self) -> None:
        """Конвертирует формат, сохраняя исходную систему координат."""
        result = gdal.Translate(
            destName=str(self.destination),
            srcDS=self.source,
            options=gdal.TranslateOptions(
                format=self.output_format,
                outputType=gdal.GDT_Int16,
            ),
        )
        if result is None:
            raise RuntimeError(
                f"GDAL не смог конвертировать {self.source_path}"
            )
        result.FlushCache()
        result = None
