"""Тесты чистого анализа NDVI отдельных полей."""

from datetime import date

import numpy as np

from processing.ndvi import NdviFieldAnalyzer


def test_analyzer_calculates_statistics_for_finite_values():
    """Анализатор исключает nodata и нечисловые значения из статистики."""
    analyzer = NdviFieldAnalyzer(nodata_value=-9999.0)
    values = np.array([[0.2, 0.4], [-9999.0, np.nan]])

    result = analyzer.analyze(values, date(2026, 7, 1), field_id=42)

    assert result is not None
    assert result.field_id == 42
    assert result.mean == np.float32(0.3)
    assert result.minimum == np.float32(0.2)
    assert result.maximum == np.float32(0.4)


def test_analyzer_returns_none_without_valid_values():
    """Поле без валидных пикселей не создаёт запись статистики."""
    analyzer = NdviFieldAnalyzer(nodata_value=-9999.0)

    result = analyzer.analyze(
        np.full((5, 5), -9999.0),
        date(2026, 7, 1),
        field_id=42,
    )

    assert result is None


def test_uniformity_accepts_smooth_vegetated_field():
    """Ровное поле с достаточным NDVI считается однородным."""
    values = np.full((20, 20), 0.5, dtype=np.float32)

    assert NdviFieldAnalyzer.is_uniform(values) is True


def test_uniformity_rejects_empty_and_low_ndvi_fields():
    """Пустое поле и поле с низким NDVI не считаются однородными."""
    assert NdviFieldAnalyzer.is_uniform(np.zeros((20, 20))) is False
    assert NdviFieldAnalyzer.is_uniform(
        np.full((20, 20), 0.1, dtype=np.float32)
    ) is False
