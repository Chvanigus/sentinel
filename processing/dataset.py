"""Управление временем жизни, блочным вводом-выводом и записью GDAL."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from osgeo import gdal

gdal.UseExceptions()

FLOAT_GTIFF_OPTIONS = (
    "TILED=YES",
    "BLOCKXSIZE=512",
    "BLOCKYSIZE=512",
    "COMPRESS=DEFLATE",
    "PREDICTOR=3",
    "BIGTIFF=IF_SAFER",
    "NUM_THREADS=ALL_CPUS",
)


@contextmanager
def open_raster(path: str | Path, mode=gdal.GA_ReadOnly):
    """Открывает GDAL dataset и освобождает ссылку после использования."""
    dataset = gdal.Open(str(path), mode)
    if dataset is None:
        raise FileNotFoundError(f"Не удалось открыть растр: {path}")
    try:
        yield dataset
    finally:
        dataset = None


@contextmanager
def atomic_raster_path(destination: str | Path):
    """Предоставляет временный путь и атомарно публикует готовый растр."""
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.unlink(missing_ok=True)
    try:
        yield str(temporary)
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def iter_raster_windows(
        dataset,
        *,
        maximum_block_size: int = 1024,
) -> Iterator[tuple[int, int, int, int]]:
    """Перечисляет окна растра, согласованные с блоками первого канала."""
    if maximum_block_size < 1:
        raise ValueError("maximum_block_size должен быть положительным")

    block_width, block_height = dataset.GetRasterBand(1).GetBlockSize()
    block_width = min(max(1, block_width), maximum_block_size)
    block_height = min(max(1, block_height), maximum_block_size)

    for y_offset in range(0, dataset.RasterYSize, block_height):
        height = min(block_height, dataset.RasterYSize - y_offset)
        for x_offset in range(0, dataset.RasterXSize, block_width):
            width = min(block_width, dataset.RasterXSize - x_offset)
            yield x_offset, y_offset, width, height


def ensure_same_grid(reference, candidate, label: str) -> None:
    """Проверяет совпадение размеров, геопривязки и проекции двух растров."""
    reference_size = (reference.RasterXSize, reference.RasterYSize)
    candidate_size = (candidate.RasterXSize, candidate.RasterYSize)
    if reference_size != candidate_size:
        raise ValueError(
            f"Растр {label} имеет другую форму: "
            f"{candidate_size} != {reference_size}"
        )
    if candidate.GetGeoTransform() != reference.GetGeoTransform():
        raise ValueError(f"Растр {label} имеет другую геопривязку")
    if candidate.GetProjection() != reference.GetProjection():
        raise ValueError(f"Растр {label} имеет другую проекцию")


@contextmanager
def create_raster_like(
        source,
        destination: str | Path,
        *,
        nodata: float | None,
        data_type: int | None = None,
        output_format: str = "GTiff",
        creation_options: tuple[str, ...] = FLOAT_GTIFF_OPTIONS,
):
    """Атомарно создаёт одноканальный растр на сетке исходного dataset."""
    target_type = data_type if data_type is not None else gdal.GDT_Float32
    with atomic_raster_path(destination) as temporary:
        driver = gdal.GetDriverByName(output_format)
        output = driver.Create(
            temporary,
            source.RasterXSize,
            source.RasterYSize,
            1,
            target_type,
            options=list(creation_options),
        )
        if output is None:
            raise RuntimeError(f"Не удалось создать растр: {destination}")
        output.SetGeoTransform(source.GetGeoTransform())
        output.SetProjection(source.GetProjection())
        band = output.GetRasterBand(1)
        if nodata is not None:
            band.SetNoDataValue(nodata)
        try:
            yield output
        finally:
            band.FlushCache()
            band = None
            output.FlushCache()
            output = None
