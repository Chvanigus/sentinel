"""Тесты блочной фильтрации NDVI по категориальной SCL-маске."""

from contextlib import nullcontext
from types import SimpleNamespace

import numpy as np

from processing.processors.cloudmask import FilterNDVIProcessor


class Band:
    """Тестовый канал, поддерживающий оконное чтение и запись."""

    def __init__(self, array):
        self.array = np.asarray(array)
        self.reads = []
        self.writes = []

    def GetBlockSize(self):
        """Возвращает блок 1×1 для проверки блочного обхода."""
        return 1, 1

    def ReadAsArray(self, x_offset, y_offset, width, height):
        """Читает и фиксирует одно окно."""
        window = (x_offset, y_offset, width, height)
        self.reads.append(window)
        return self.array[
            y_offset:y_offset + height,
            x_offset:x_offset + width,
        ]

    def WriteArray(self, array, x_offset, y_offset):
        """Записывает и фиксирует одно окно."""
        height, width = array.shape
        self.array[
            y_offset:y_offset + height,
            x_offset:x_offset + width,
        ] = array
        self.writes.append((x_offset, y_offset, width, height))


class Dataset:
    """Минимальный dataset с общей сеткой."""

    def __init__(self, array):
        self.band = Band(array)
        self.RasterYSize, self.RasterXSize = self.band.array.shape

    def GetRasterBand(self, _number):
        """Возвращает единственный канал."""
        return self.band

    def GetGeoTransform(self):
        """Возвращает тестовую геопривязку."""
        return 0.0, 10.0, 0.0, 20.0, 0.0, -10.0

    def GetProjection(self):
        """Возвращает тестовую проекцию."""
        return "EPSG:3857"


class Paths:
    """Возвращает файлы одного тестового хозяйства."""

    def __init__(self, root):
        self.ndvi_path = root / "ndvi.tif"
        self.scl_path = root / "scl.tif"
        self.result_path = root / "filtered.tif"
        self.ndvi_path.touch()
        self.scl_path.touch()

    def ndvi(self, _agroid):
        """Возвращает NDVI."""
        return str(self.ndvi_path)

    def scl_10m(self, _agroid):
        """Возвращает SCL на сетке NDVI."""
        return str(self.scl_path)

    def filtered_ndvi(self, _agroid):
        """Возвращает фильтрованный NDVI."""
        return str(self.result_path)


def test_filter_ndvi_processes_matching_rasters_by_blocks(
        tmp_path,
        monkeypatch,
):
    """Фильтр не загружает растр целиком и сохраняет только валидные классы."""
    paths = Paths(tmp_path)
    ndvi = Dataset([[0.2, 0.7], [np.nan, 0.5]])
    scl = Dataset([[4, 3], [5, 7]])
    output = Dataset(np.empty((2, 2), dtype=np.float32))
    datasets = {
        str(paths.ndvi_path): ndvi,
        str(paths.scl_path): scl,
    }
    monkeypatch.setattr(
        "processing.processors.cloudmask.open_raster",
        lambda path: nullcontext(datasets[path]),
    )
    monkeypatch.setattr(
        "processing.processors.cloudmask.create_raster_like",
        lambda *_args, **_kwargs: nullcontext(output),
    )

    FilterNDVIProcessor(
        SimpleNamespace(agroids=(3,)),
        paths,
        SimpleNamespace(nodata=-42.0),
    ).run()

    expected_windows = [
        (0, 0, 1, 1),
        (1, 0, 1, 1),
        (0, 1, 1, 1),
        (1, 1, 1, 1),
    ]
    assert ndvi.band.reads == expected_windows
    assert scl.band.reads == expected_windows
    assert output.band.writes == expected_windows
    np.testing.assert_allclose(
        output.band.array,
        [[0.2, -42.0], [-42.0, 0.5]],
    )
