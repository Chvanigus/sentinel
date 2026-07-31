"""Тесты чистых математических операций над растровыми массивами."""

import numpy as np
import pytest

from processing.calculations import apply_scl_mask, normalized_difference


def test_normalized_difference_calculates_and_clips_values():
    """Нормализованная разность рассчитывается и ограничивается единицей."""
    primary = np.array([[8, 3, 10]], dtype=np.float64)
    secondary = np.array([[2, 1, -5]], dtype=np.float64)

    result = normalized_difference(primary, secondary)

    np.testing.assert_allclose(result, [[0.6, 0.5, 1.0]])
    assert result.dtype == np.float32


def test_normalized_difference_marks_zero_and_nan_as_nodata():
    """Нулевой знаменатель и NaN заменяются настроенным nodata."""
    primary = np.array([[1.0, np.nan]])
    secondary = np.array([[-1.0, 2.0]])

    result = normalized_difference(primary, secondary, nodata=-42.0)

    np.testing.assert_array_equal(result, [[-42.0, -42.0]])


def test_normalized_difference_rejects_different_shapes():
    """Массивы разной формы отклоняются до вычисления."""
    with pytest.raises(ValueError, match="одинаковую форму"):
        normalized_difference(np.ones((2, 2)), np.ones((2, 3)))


def test_scl_mask_keeps_only_valid_finite_ndvi():
    """SCL-маска сохраняет допустимые конечные значения NDVI."""
    ndvi = np.array([[0.5, 0.7], [np.nan, 0.3]])
    scl = np.array([[4, 3], [5, 7]])

    result = apply_scl_mask(ndvi, scl, nodata=-42.0)

    np.testing.assert_allclose(result, [[0.5, -42.0], [-42.0, 0.3]])
    assert result.dtype == np.float32


def test_scl_mask_rejects_different_shapes():
    """NDVI и SCL на разных сетках отклоняются до фильтрации."""
    with pytest.raises(ValueError, match="одинаковую форму"):
        apply_scl_mask(np.ones((2, 2)), np.ones((2, 3)))
