"""Тесты строгого разбора имён публикуемых растров."""

from datetime import date

import pytest

from satgeo.models import split_file_name


def test_split_file_name_parses_processed_layer():
    """Разборщик извлекает основные атрибуты обработанного слоя."""
    info = split_file_name(
        "s2a_01_07_2026_a3_ndvi_10m_3857.tif"
    )

    assert info.satellite == "s2a"
    assert info.date() == date(2026, 7, 1)
    assert info.agroid == "3"
    assert info.img_type == "ndvi"
    assert info.resolution == 10


def test_split_file_name_supports_optional_field_id():
    """Разборщик поддерживает необязательный идентификатор поля."""
    info = split_file_name(
        "S2B_31_12_2025_A5_F42_TCI_10m_3857.tif"
    )

    assert info.field_id == "F42"
    assert info.img_type == "tci"


@pytest.mark.parametrize(
    "name",
    [
        "garbage.tif",
        "s2a_99_99_2026_a3_ndvi_10m_3857.tif",
        "s2a_01_07_2026_ndvi_10m_3857.tif",
        "s2a_01_07_2026_a3_unknown_10m_3857.tif",
    ],
)
def test_split_file_name_rejects_invalid_names(name):
    """Некорректные форматы имени слоя отклоняются."""
    with pytest.raises(ValueError):
        split_file_name(name)
