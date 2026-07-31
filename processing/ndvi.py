"""Чистый анализ статистики и однородности NDVI-массивов."""
from __future__ import annotations

from datetime import date

import cv2
import numpy as np
from scipy.ndimage import binary_erosion

from domain.models import NdviStatistics


class NdviFieldAnalyzer:
    """Рассчитывает статистику и оценивает однородность NDVI поля."""

    def __init__(self, nodata_value: float = -9999.0):
        self.nodata_value = nodata_value

    def analyze(
            self,
            ndvi: np.ndarray | None,
            acquired_on: date,
            field_id: int,
    ) -> NdviStatistics | None:
        """Возвращает статистику поля либо ``None`` при отсутствии данных."""
        if ndvi is None:
            return None

        ndvi_array = np.asarray(ndvi, dtype=np.float32)
        valid_mask = (
            (ndvi_array != self.nodata_value)
            & np.isfinite(ndvi_array)
        )
        if not np.any(valid_mask):
            return None

        values = ndvi_array[valid_mask]
        mean = float(np.mean(values))
        standard_deviation = float(np.std(values))
        coefficient_of_variation = (
            standard_deviation / abs(mean) * 100 if mean else 0.0
        )

        return NdviStatistics(
            acquired_on=acquired_on,
            field_id=field_id,
            mean=mean,
            maximum=float(np.max(values)),
            minimum=float(np.min(values)),
            growth_percent=0.0,
            coefficient_of_variation=coefficient_of_variation,
            is_uniform=self.is_uniform(ndvi_array),
        )

    @staticmethod
    def is_uniform(ndvi: np.ndarray) -> bool:
        """Оценивает статистическую и текстурную однородность поля."""
        ndvi_array = np.asarray(ndvi, dtype=np.float32)
        valid_mask = (ndvi_array > 0) & np.isfinite(ndvi_array)
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
        edge_ratio = float(np.count_nonzero(edges)) / edges.size

        statistically_uniform = (
            coefficient_of_variation < 20
            and median_absolute_deviation < 0.03
        )
        return statistically_uniform and edge_ratio < 0.02
