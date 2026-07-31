"""Чистые математические операции над растровыми массивами."""

import numpy as np


def normalized_difference(
        primary: np.ndarray,
        secondary: np.ndarray,
        *,
        nodata: float = -9999.0,
) -> np.ndarray:
    """Рассчитывает нормализованную разность двух одинаковых по форме бандов."""
    primary_array = np.asarray(primary, dtype=np.float32)
    secondary_array = np.asarray(secondary, dtype=np.float32)
    if primary_array.shape != secondary_array.shape:
        raise ValueError(
            "Спектральные банды должны иметь одинаковую форму: "
            f"{primary_array.shape} != {secondary_array.shape}"
        )

    denominator = primary_array + secondary_array
    result = np.full(primary_array.shape, nodata, dtype=np.float32)
    with np.errstate(divide="ignore", invalid="ignore"):
        np.divide(
            primary_array - secondary_array,
            denominator,
            out=result,
            where=denominator != 0,
        )

    valid = np.isfinite(result) & (result != nodata)
    result[valid] = np.clip(result[valid], -1.0, 1.0)
    result[~np.isfinite(result)] = nodata
    return result


def apply_scl_mask(
        ndvi: np.ndarray,
        scl: np.ndarray,
        *,
        valid_classes: tuple[int, ...] = (4, 5, 6, 7),
        nodata: float = -9999.0,
) -> np.ndarray:
    """Оставляет NDVI только для допустимых классов поверхности SCL."""
    ndvi_array = np.asarray(ndvi, dtype=np.float32)
    scl_array = np.asarray(scl)
    if ndvi_array.shape != scl_array.shape:
        raise ValueError(
            "NDVI и SCL должны иметь одинаковую форму: "
            f"{ndvi_array.shape} != {scl_array.shape}"
        )

    valid = np.isin(scl_array, valid_classes) & np.isfinite(ndvi_array)
    result = np.full(ndvi_array.shape, nodata, dtype=np.float32)
    np.copyto(result, ndvi_array, where=valid)
    return result
