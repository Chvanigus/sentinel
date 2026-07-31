"""Тесты общих примитивов блочной и атомарной работы с растрами."""

from pathlib import Path

import pytest

from processing.dataset import (
    atomic_raster_path,
    ensure_same_grid,
    iter_raster_windows,
)


class Band:
    """Минимальный канал с настраиваемым размером блока."""

    def __init__(self, block_size):
        self.block_size = block_size

    def GetBlockSize(self):
        """Возвращает размер физического блока."""
        return self.block_size


class Dataset:
    """Минимальный dataset для тестирования сетки и окон."""

    def __init__(
            self,
            *,
            width=5,
            height=3,
            block_size=(2, 2),
            transform=(0.0, 10.0, 0.0, 30.0, 0.0, -10.0),
            projection="EPSG:3857",
    ):
        self.RasterXSize = width
        self.RasterYSize = height
        self.band = Band(block_size)
        self.transform = transform
        self.projection = projection

    def GetRasterBand(self, _number):
        """Возвращает тестовый канал."""
        return self.band

    def GetGeoTransform(self):
        """Возвращает геопривязку."""
        return self.transform

    def GetProjection(self):
        """Возвращает проекцию."""
        return self.projection


def test_iter_raster_windows_covers_edges_without_full_array():
    """Окна покрывают растр и корректно уменьшаются на правом и нижнем краю."""
    assert list(iter_raster_windows(Dataset())) == [
        (0, 0, 2, 2),
        (2, 0, 2, 2),
        (4, 0, 1, 2),
        (0, 2, 2, 1),
        (2, 2, 2, 1),
        (4, 2, 1, 1),
    ]


def test_iter_raster_windows_caps_oversized_source_blocks():
    """Слишком крупный исходный блок ограничивается настройкой памяти."""
    windows = list(
        iter_raster_windows(
            Dataset(width=1500, height=1, block_size=(4096, 4096)),
            maximum_block_size=1024,
        )
    )

    assert windows == [(0, 0, 1024, 1), (1024, 0, 476, 1)]


@pytest.mark.parametrize(
    ("candidate", "message"),
    [
        (Dataset(width=4), "форму"),
        (
            Dataset(transform=(1.0, 10.0, 0.0, 30.0, 0.0, -10.0)),
            "геопривязку",
        ),
        (Dataset(projection="EPSG:32638"), "проекцию"),
    ],
)
def test_ensure_same_grid_rejects_mismatch(candidate, message):
    """Сетка каналов проверяется до начала дорогостоящего вычисления."""
    with pytest.raises(ValueError, match=message):
        ensure_same_grid(Dataset(), candidate, "B04")


def test_atomic_raster_path_replaces_only_completed_file(tmp_path):
    """Готовый временный растр атомарно становится целевым."""
    destination = tmp_path / "result.tif"

    with atomic_raster_path(destination) as temporary:
        assert temporary.endswith(".tif.partial")
        Path(temporary).write_bytes(b"complete")
        assert not destination.exists()

    assert destination.read_bytes() == b"complete"
    assert not Path(f"{destination}.partial").exists()


def test_atomic_raster_path_removes_partial_after_failure(tmp_path):
    """Ошибка не оставляет файл, который следующий запуск принял бы за готовый."""
    destination = tmp_path / "result.tif"

    with pytest.raises(RuntimeError, match="broken"):
        with atomic_raster_path(destination) as temporary:
            Path(temporary).write_bytes(b"partial")
            raise RuntimeError("broken")

    assert not destination.exists()
    assert not Path(f"{destination}.partial").exists()
