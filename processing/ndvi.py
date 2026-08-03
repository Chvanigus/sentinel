"""Чистый анализ статистики и однородности NDVI-массивов."""
from __future__ import annotations

from datetime import UTC, date, datetime

import cv2
import numpy as np
from scipy.ndimage import binary_erosion

from domain.models import NdviStatistics

NDVI_ALGORITHM_VERSION = "2.0.0"
CLOUD_SCL_CLASSES = (8, 9, 10)
SHADOW_SCL_CLASSES = (2, 3)
SNOW_SCL_CLASSES = (11,)
CLEAR_SCL_CLASSES = (4, 5, 6, 7)


class NdviFieldAnalyzer:
    """Рассчитывает статистику и оценивает однородность NDVI поля."""

    def __init__(self, nodata_value: float = -9999.0):
        self.nodata_value = nodata_value

    def analyze(
            self,
            ndvi: np.ndarray | None,
            acquired_on: date,
            field_id: int,
            coverage_mask: np.ndarray | None = None,
            scl: np.ndarray | None = None,
            source_level: str | None = None,
    ) -> NdviStatistics | None:
        """Возвращает статистику и показатели качества пикселей поля."""
        if ndvi is None:
            return None

        ndvi_array = np.asarray(ndvi, dtype=np.float32)
        coverage = (
            np.ones(ndvi_array.shape, dtype=bool)
            if coverage_mask is None
            else np.asarray(coverage_mask, dtype=bool)
        )
        if coverage.shape != ndvi_array.shape:
            raise ValueError(
                "Маска поля и NDVI должны иметь одинаковую форму"
            )
        total_pixel_count = int(np.count_nonzero(coverage))
        if total_pixel_count == 0:
            return None

        valid_mask = (
            coverage
            & (ndvi_array != self.nodata_value)
            & np.isfinite(ndvi_array)
            & (ndvi_array >= -1.0)
            & (ndvi_array <= 1.0)
        )
        cloud_pixel_count = None
        shadow_pixel_count = None
        snow_pixel_count = None
        cloud_coverage_percent = None
        excluded_mask = np.zeros(ndvi_array.shape, dtype=bool)
        if scl is not None:
            scl_array = np.asarray(scl)
            if scl_array.shape != ndvi_array.shape:
                raise ValueError(
                    "SCL и NDVI должны иметь одинаковую форму"
                )
            cloud_mask = coverage & np.isin(
                scl_array,
                CLOUD_SCL_CLASSES,
            )
            shadow_mask = coverage & np.isin(
                scl_array,
                SHADOW_SCL_CLASSES,
            )
            snow_mask = coverage & np.isin(
                scl_array,
                SNOW_SCL_CLASSES,
            )
            clear_mask = coverage & np.isin(
                scl_array,
                CLEAR_SCL_CLASSES,
            )
            valid_mask &= clear_mask
            excluded_mask = cloud_mask | shadow_mask | snow_mask
            cloud_pixel_count = int(np.count_nonzero(cloud_mask))
            shadow_pixel_count = int(np.count_nonzero(shadow_mask))
            snow_pixel_count = int(np.count_nonzero(snow_mask))
            cloud_coverage_percent = (
                cloud_pixel_count / total_pixel_count * 100
            )

        nodata_mask = coverage & ~valid_mask & ~excluded_mask
        values = ndvi_array[valid_mask]
        valid_pixel_count = int(values.size)
        nodata_pixel_count = int(np.count_nonzero(nodata_mask))
        valid_coverage_percent = (
            valid_pixel_count / total_pixel_count * 100
        )

        mean = float(np.mean(values)) if values.size else None
        standard_deviation = (
            float(np.std(values)) if values.size else None
        )
        coefficient_of_variation = (
            standard_deviation / abs(mean) * 100
            if mean not in (None, 0.0)
            else None
        )

        return NdviStatistics(
            acquired_on=acquired_on,
            field_id=field_id,
            mean=mean,
            maximum=float(np.max(values)) if values.size else None,
            minimum=float(np.min(values)) if values.size else None,
            growth_percent=None,
            coefficient_of_variation=coefficient_of_variation,
            is_uniform=self.is_uniform(ndvi_array, coverage),
            valid_pixel_count=valid_pixel_count,
            total_pixel_count=total_pixel_count,
            cloud_pixel_count=cloud_pixel_count,
            nodata_pixel_count=nodata_pixel_count,
            shadow_pixel_count=shadow_pixel_count,
            snow_pixel_count=snow_pixel_count,
            valid_coverage_percent=valid_coverage_percent,
            cloud_coverage_percent=cloud_coverage_percent,
            standard_deviation=standard_deviation,
            median=float(np.median(values)) if values.size else None,
            percentile_10=(
                float(np.percentile(values, 10)) if values.size else None
            ),
            percentile_90=(
                float(np.percentile(values, 90)) if values.size else None
            ),
            source_level=source_level,
            algorithm_version=NDVI_ALGORITHM_VERSION,
            calculated_at=datetime.now(UTC),
        )

    @staticmethod
    def is_uniform(
            ndvi: np.ndarray,
            coverage_mask: np.ndarray | None = None,
    ) -> bool:
        """Оценивает статистическую и текстурную однородность поля."""
        ndvi_array = np.asarray(ndvi, dtype=np.float32)
        coverage = (
            np.ones(ndvi_array.shape, dtype=bool)
            if coverage_mask is None
            else np.asarray(coverage_mask, dtype=bool)
        )
        if coverage.shape != ndvi_array.shape:
            raise ValueError(
                "Маска поля и NDVI должны иметь одинаковую форму"
            )
        valid_mask = (
            coverage
            & (ndvi_array > 0)
            & (ndvi_array <= 1.0)
            & np.isfinite(ndvi_array)
        )
        if not np.any(valid_mask):
            return False

        eroded_mask = binary_erosion(
            valid_mask,
            structure=np.ones((5, 5)),
        )
        values = ndvi_array[eroded_mask]
        if values.size == 0:
            return False

        mean = float(np.mean(values))
        if mean < 0.15:
            return False

        coefficient_of_variation = float(np.std(values)) / mean * 100
        median = float(np.median(values))
        median_absolute_deviation = float(
            np.median(np.abs(values - median))
        )

        filled = np.copy(ndvi_array)
        filled[~valid_mask] = mean
        blurred = cv2.GaussianBlur(filled, (5, 5), sigmaX=1.0)
        image = np.clip(blurred * 255, 0, 255).astype(np.uint8)
        edges = cv2.Canny(image, threshold1=20, threshold2=60)
        edge_ratio = float(np.count_nonzero(edges[eroded_mask])) / values.size

        statistically_uniform = (
            coefficient_of_variation < 20
            and median_absolute_deviation < 0.03
        )
        return statistically_uniform and edge_ratio < 0.02
