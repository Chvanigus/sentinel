"""Тесты блочного вычисления спектральных индексов."""

from contextlib import nullcontext

import numpy as np
import pytest

from processing.indexes import SpectralIndexProcessor


class InputBand:
    """Имитирует один исходный канал и записывает прочитанные окна."""

    def __init__(self, array):
        self.array = np.asarray(array)
        self.reads = []

    def GetBlockSize(self):
        """Возвращает маленький блок для проверки оконного режима."""
        return 1, 1

    def ReadAsArray(self, x_offset, y_offset, width, height):
        """Возвращает запрошенное окно исходного массива."""
        window = (x_offset, y_offset, width, height)
        self.reads.append(window)
        return self.array[
            y_offset:y_offset + height,
            x_offset:x_offset + width,
        ]


class InputDataset:
    """Минимальный исходный dataset с общей тестовой сеткой."""

    def __init__(self, array):
        self.band = InputBand(array)
        self.RasterYSize, self.RasterXSize = self.band.array.shape

    def GetRasterBand(self, _number):
        """Возвращает единственный канал."""
        return self.band

    def GetGeoTransform(self):
        """Возвращает общую тестовую геопривязку."""
        return 0.0, 10.0, 0.0, 20.0, 0.0, -10.0

    def GetProjection(self):
        """Возвращает общую тестовую проекцию."""
        return "EPSG:3857"


class OutputBand:
    """Собирает блочные записи в единый результирующий массив."""

    def __init__(self, shape):
        self.array = np.empty(shape, dtype=np.float32)
        self.writes = []

    def WriteArray(self, array, x_offset, y_offset):
        """Записывает массив в указанное окно результата."""
        height, width = array.shape
        self.array[
            y_offset:y_offset + height,
            x_offset:x_offset + width,
        ] = array
        self.writes.append((x_offset, y_offset, width, height))


class OutputDataset:
    """Минимальный выходной dataset."""

    def __init__(self, shape):
        self.band = OutputBand(shape)

    def GetRasterBand(self, _number):
        """Возвращает единственный выходной канал."""
        return self.band


def configure_rasters(monkeypatch):
    """Подменяет GDAL datasets детерминированными тестовыми объектами."""
    inputs = {
        "b03.jp2": InputDataset([[2.0, 8.0]]),
        "b04.jp2": InputDataset([[2.0, 4.0]]),
        "b08.jp2": InputDataset([[8.0, 4.0]]),
    }
    outputs = {}

    monkeypatch.setattr(
        "processing.indexes.open_raster",
        lambda path: nullcontext(inputs[path]),
    )

    def create(_source, destination, **_options):
        """Создаёт выходной dataset для указанного индекса."""
        output = OutputDataset((1, 2))
        outputs[destination] = output
        return nullcontext(output)

    monkeypatch.setattr("processing.indexes.create_raster_like", create)
    return inputs, outputs


def test_create_processes_blocks_and_reuses_b08(monkeypatch):
    """Оба индекса используют одно чтение B08 на каждое окно."""
    inputs, outputs = configure_rasters(monkeypatch)
    processor = SpectralIndexProcessor(
        b03_file="b03.jp2",
        b04_file="b04.jp2",
        b08_file="b08.jp2",
        nodata=-42.0,
    )

    processor.create({"ndvi": "ndvi.tif", "ndwi": "ndwi.tif"})

    expected_windows = [(0, 0, 1, 1), (1, 0, 1, 1)]
    assert inputs["b08.jp2"].band.reads == expected_windows
    assert inputs["b04.jp2"].band.reads == expected_windows
    assert inputs["b03.jp2"].band.reads == expected_windows
    assert outputs["ndvi.tif"].band.writes == expected_windows
    assert outputs["ndwi.tif"].band.writes == expected_windows
    np.testing.assert_allclose(
        outputs["ndvi.tif"].band.array,
        [[0.6, 0.0]],
    )
    np.testing.assert_allclose(
        outputs["ndwi.tif"].band.array,
        [[-0.6, 1.0 / 3.0]],
    )


def test_create_returns_without_opening_for_empty_outputs(monkeypatch):
    """Пустой набор результатов не открывает исходные растры."""
    monkeypatch.setattr(
        "processing.indexes.open_raster",
        lambda _path: pytest.fail("Исходники не должны читаться"),
    )

    SpectralIndexProcessor(b08_file="b08.jp2").create({})


def test_create_rejects_unknown_index_before_opening(monkeypatch):
    """Неизвестный индекс отклоняется до открытия исходных файлов."""
    monkeypatch.setattr(
        "processing.indexes.open_raster",
        lambda _path: pytest.fail("Исходники не должны читаться"),
    )

    with pytest.raises(ValueError, match="Неподдерживаемые"):
        SpectralIndexProcessor(b08_file="b08.jp2").create(
            {"evi": "evi.tif"}
        )


@pytest.mark.parametrize(
    ("outputs", "message"),
    [
        ({"ndvi": "ndvi.tif"}, "B04"),
        ({"ndwi": "ndwi.tif"}, "B03"),
    ],
)
def test_create_requires_secondary_band(outputs, message, monkeypatch):
    """Для выбранного индекса обязательно задаётся его второй канал."""
    monkeypatch.setattr(
        "processing.indexes.open_raster",
        lambda _path: pytest.fail("Исходники не должны читаться"),
    )

    with pytest.raises(ValueError, match=message):
        SpectralIndexProcessor(b08_file="b08.jp2").create(outputs)
