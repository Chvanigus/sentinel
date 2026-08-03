"""Чистые математические операции над растровыми массивами."""

import numpy as np


def normalized_difference(
        primary: np.ndarray,
        secondary: np.ndarray,
        *,
        primary_offset: float = 0.0,
        secondary_offset: float = 0.0,
        source_nodata: float = 0.0,
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

    valid = (
        np.isfinite(primary_array)
        & np.isfinite(secondary_array)
        & (primary_array != source_nodata)
        & (secondary_array != source_nodata)
    )
    # Два рабочих float32-массива вместо отдельных копий обеих отражательных
    # способностей, знаменателя и результата существенно снижают нагрузку на
    # память при обработке больших окон Sentinel.
    result = np.subtract(primary_array, secondary_array, dtype=np.float32)
    offset_difference = primary_offset - secondary_offset
    if offset_difference:
        result += offset_difference
    denominator = np.add(
        primary_array,
        secondary_array,
        dtype=np.float32,
    )
    offset_sum = primary_offset + secondary_offset
    if offset_sum:
        denominator += offset_sum
    valid &= denominator != 0
    with np.errstate(divide="ignore", invalid="ignore"):
        np.divide(
            result,
            denominator,
            out=result,
            where=valid,
        )

    np.clip(result, -1.0, 1.0, out=result, where=valid)
    result[~valid] = nodata
    return result
