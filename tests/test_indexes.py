"""Тесты вычисления и записи спектральных индексов."""

import numpy as np
import pytest

from processing.indexes import SpectralIndexProcessor


class RecordingWriter:
    """Запоминает параметры и массивы вместо записи растра через GDAL."""

    writes = []

    def __init__(self, *, destination, source, nodata):
        """Сохраняет метаданные будущего выходного растра."""
        self.destination = destination
        self.source = source
        self.nodata = nodata

    def write(self, array):
        """Запоминает копию записываемого массива."""
        self.writes.append(
            {
                "destination": self.destination,
                "source": self.source,
                "nodata": self.nodata,
                "array": array.copy(),
            }
        )


def test_create_reuses_b08_for_ndvi_and_ndwi(monkeypatch):
    """Оба индекса читают общий B08 только один раз."""
    bands = {
        "b03.jp2": np.array([[2.0, 8.0]]),
        "b04.jp2": np.array([[2.0, 4.0]]),
        "b08.jp2": np.array([[8.0, 4.0]]),
    }
    reads = []

    def load_band(path):
        """Возвращает тестовый канал и фиксирует обращение к нему."""
        reads.append(path)
        return bands[path]

    RecordingWriter.writes = []
    monkeypatch.setattr(
        SpectralIndexProcessor,
        "_load_band",
        staticmethod(load_band),
    )
    monkeypatch.setattr(
        "processing.indexes.RasterArrayWriter",
        RecordingWriter,
    )
    processor = SpectralIndexProcessor(
        b03_file="b03.jp2",
        b04_file="b04.jp2",
        b08_file="b08.jp2",
        nodata=-42.0,
    )

    processor.create(
        {
            "ndvi": "ndvi.tif",
            "ndwi": "ndwi.tif",
        }
    )

    assert reads == ["b08.jp2", "b04.jp2", "b03.jp2"]
    assert [item["destination"] for item in RecordingWriter.writes] == [
        "ndvi.tif",
        "ndwi.tif",
    ]
    assert [item["source"] for item in RecordingWriter.writes] == [
        "b08.jp2",
        "b03.jp2",
    ]
    assert all(item["nodata"] == -42.0 for item in RecordingWriter.writes)
    np.testing.assert_allclose(
        RecordingWriter.writes[0]["array"],
        [[0.6, 0.0]],
    )
    np.testing.assert_allclose(
        RecordingWriter.writes[1]["array"],
        [[-0.6, 1.0 / 3.0]],
    )


def test_create_returns_without_reading_for_empty_outputs(monkeypatch):
    """Пустой набор результатов не открывает исходные растры."""
    processor = SpectralIndexProcessor(b08_file="b08.jp2")
    monkeypatch.setattr(
        processor,
        "_load_band",
        lambda _path: pytest.fail("Исходники не должны читаться"),
    )

    processor.create({})


def test_create_rejects_unknown_index_before_reading(monkeypatch):
    """Неизвестный индекс отклоняется до открытия исходных файлов."""
    processor = SpectralIndexProcessor(b08_file="b08.jp2")
    monkeypatch.setattr(
        processor,
        "_load_band",
        lambda _path: pytest.fail("Исходники не должны читаться"),
    )

    with pytest.raises(ValueError, match="Неподдерживаемые"):
        processor.create({"evi": "evi.tif"})


@pytest.mark.parametrize(
    ("outputs", "message"),
    [
        ({"ndvi": "ndvi.tif"}, "B04"),
        ({"ndwi": "ndwi.tif"}, "B03"),
    ],
)
def test_create_requires_secondary_band(outputs, message, monkeypatch):
    """Для выбранного индекса обязательно задаётся его второй канал."""
    processor = SpectralIndexProcessor(b08_file="b08.jp2")
    monkeypatch.setattr(
        processor,
        "_load_band",
        lambda _path: np.ones((1, 1), dtype=np.float32),
    )

    with pytest.raises(ValueError, match=message):
        processor.create(outputs)
