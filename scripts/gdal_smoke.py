"""Автономная проверка ключевой GDAL-цепочки на синтетических растрах."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import numpy as np
from osgeo import gdal, osr

from processing.domain import ProductLevel, SceneContext
from processing.indexes import SpectralIndexProcessor
from processing.processors.cloudmask import (
    FilterNDVIProcessor,
    RescaleSCLProcessor,
)
from processing.processors.combine import MosaicProcessor
from processing.processors.sentinel import AgroCropProcessor

gdal.UseExceptions()


def write_raster(
        path: Path,
        values: np.ndarray,
        *,
        pixel_size: float,
        data_type: int,
        origin_x: float = 0.0,
) -> None:
    """Записывает небольшой одноканальный GeoTIFF в EPSG:3857."""
    driver = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(
        str(path),
        values.shape[1],
        values.shape[0],
        1,
        data_type,
    )
    if dataset is None:
        raise RuntimeError(f"Не удалось создать тестовый растр: {path}")

    spatial_reference = osr.SpatialReference()
    spatial_reference.ImportFromEPSG(3857)
    dataset.SetProjection(spatial_reference.ExportToWkt())
    dataset.SetGeoTransform(
        (origin_x, pixel_size, 0.0, 40.0, 0.0, -pixel_size)
    )
    band = dataset.GetRasterBand(1)
    band.WriteArray(values)
    band.FlushCache()
    band = None
    dataset.FlushCache()
    dataset = None


def read_raster(path: Path) -> np.ndarray:
    """Читает тестовый растр в NumPy-массив."""
    dataset = gdal.Open(str(path))
    if dataset is None:
        raise FileNotFoundError(path)
    try:
        return dataset.ReadAsArray()
    finally:
        dataset = None


class SmokeCloudMaskPaths:
    """Минимальные пути, необходимые процессорам облачной маски."""

    def __init__(self, root: Path):
        self._ndvi = root / "ndvi.tif"
        self._scl_20m = root / "scl_20m.tif"
        self._scl_10m = root / "scl_10m.tif"
        self._filtered = root / "ndvi_filtered.tif"

    def ndvi(self, _agroid: int) -> str:
        """Возвращает исходный NDVI."""
        return str(self._ndvi)

    def scl_20m(self, _agroid: int) -> str:
        """Возвращает исходную SCL-маску."""
        return str(self._scl_20m)

    def scl_10m(self, _agroid: int) -> str:
        """Возвращает SCL-маску на сетке NDVI."""
        return str(self._scl_10m)

    def filtered_ndvi(self, _agroid: int) -> str:
        """Возвращает результат фильтрации NDVI."""
        return str(self._filtered)


class SmokeCropPaths:
    """Пути одного синтетического растра и результатов crop."""

    def __init__(self, root: Path, source: Path):
        self._root = root
        self._source = source

    def sources(self, _stage: str) -> list[str]:
        """Возвращает общий тестовый источник для любого продукта."""
        return [str(self._source)]

    def destination(self, stage: str, agroid: int) -> str:
        """Возвращает отдельный crop-результат продукта."""
        return str(self._root / f"{stage}_a{agroid}.tif")


class SmokeFieldData:
    """Фиксированные границы хозяйства для crop smoke-test."""

    def __init__(self):
        self.bounds_calls = 0

    def bounds(
            self,
            *,
            year: int,
            agroid: int,
            srid: int,
    ) -> tuple[float, float, float, float]:
        """Возвращает центральную область синтетического растра."""
        if (year, agroid, srid) != (2026, 3, 3857):
            raise AssertionError("Crop processor передал неверный контекст")
        self.bounds_calls += 1
        return 10.0, 10.0, 30.0, 30.0


class SmokeMosaicPaths:
    """Пути двух соседних тайлов и результатов mosaic."""

    def __init__(self, root: Path, sources: list[Path]):
        self._root = root
        self._sources = sources

    def sources(self, _product: str) -> list[str]:
        """Возвращает два соседних синтетических тайла."""
        return [str(path) for path in self._sources]

    def destination(self, product: str) -> str:
        """Возвращает путь объединённого продукта."""
        return str(self._root / f"mosaic_{product}.tif")


def make_scene(
        *,
        level: ProductLevel = ProductLevel.L2A,
        agroids: tuple[int, ...] = (1,),
) -> SceneContext:
    """Создаёт общий контекст синтетической сцены."""
    return SceneContext(
        archive_path=Path("smoke.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=level,
        agroids=agroids,
    )


def run_cloud_mask_smoke(root: Path) -> None:
    """Проверяет ресемплинг SCL и фильтрацию NDVI реальным GDAL."""
    root.mkdir()
    paths = SmokeCloudMaskPaths(root)
    ndvi = np.array(
        [
            [0.2, 0.2, 0.8, 0.8],
            [0.2, 0.2, 0.8, 0.8],
            [0.5, 0.5, 0.7, 0.7],
            [0.5, 0.5, 0.7, 0.7],
        ],
        dtype=np.float32,
    )
    scl = np.array(
        [
            [4, 3],
            [5, 7],
        ],
        dtype=np.int16,
    )
    write_raster(
        Path(paths.ndvi(1)),
        ndvi,
        pixel_size=10,
        data_type=gdal.GDT_Float32,
    )
    write_raster(
        Path(paths.scl_20m(1)),
        scl,
        pixel_size=20,
        data_type=gdal.GDT_Int16,
    )

    scene = make_scene()
    RescaleSCLProcessor(scene, paths).run()
    FilterNDVIProcessor(
        scene,
        paths,
        SimpleNamespace(nodata=-9999.0),
    ).run()

    rescaled_scl = read_raster(Path(paths.scl_10m(1)))
    filtered_ndvi = read_raster(Path(paths.filtered_ndvi(1)))
    if rescaled_scl.shape != ndvi.shape:
        raise AssertionError(
            f"Сетка SCL не совпала с NDVI: "
            f"{rescaled_scl.shape} != {ndvi.shape}"
        )
    if not np.all(filtered_ndvi[:2, 2:] == -9999.0):
        raise AssertionError(
            "Пиксели класса SCL=3 не были замаскированы"
        )
    if not np.allclose(filtered_ndvi[2:, :], ndvi[2:, :]):
        raise AssertionError(
            "Допустимые классы SCL изменили значения NDVI"
        )


def run_spectral_index_smoke(root: Path) -> None:
    """Проверяет чтение каналов и запись Float32 спектрального индекса."""
    root.mkdir()
    b03 = root / "b03.tif"
    b04 = root / "b04.tif"
    b08 = root / "b08.tif"
    ndvi_destination = root / "ndvi.tif"
    ndwi_destination = root / "ndwi.tif"
    write_raster(
        b03,
        np.full((4, 4), 3, dtype=np.int16),
        pixel_size=10,
        data_type=gdal.GDT_Int16,
    )
    write_raster(
        b04,
        np.full((4, 4), 2, dtype=np.int16),
        pixel_size=10,
        data_type=gdal.GDT_Int16,
    )
    write_raster(
        b08,
        np.full((4, 4), 4, dtype=np.int16),
        pixel_size=10,
        data_type=gdal.GDT_Int16,
    )

    processor = SpectralIndexProcessor(
        b03_file=str(b03),
        b04_file=str(b04),
        b08_file=str(b08),
    )
    original_loader = processor._load_band
    loaded_paths = []

    def counting_loader(path: str) -> np.ndarray:
        """Запоминает каждое фактическое чтение спектрального канала."""
        loaded_paths.append(path)
        return original_loader(path)

    processor._load_band = counting_loader
    processor.create(
        {
            "ndvi": str(ndvi_destination),
            "ndwi": str(ndwi_destination),
        }
    )

    ndvi = read_raster(ndvi_destination)
    ndwi = read_raster(ndwi_destination)
    if not np.allclose(ndvi, 1 / 3):
        raise AssertionError("NDVI рассчитан неверно")
    if not np.allclose(ndwi, -1 / 7):
        raise AssertionError("NDWI рассчитан неверно")
    if loaded_paths.count(str(b08)) != 1:
        raise AssertionError("Общий канал B08 был прочитан повторно")


def run_crop_smoke(root: Path) -> None:
    """Проверяет crop по bounds и кеширование границ хозяйства."""
    root.mkdir()
    source = root / "source.tif"
    write_raster(
        source,
        np.arange(16, dtype=np.float32).reshape(4, 4),
        pixel_size=10,
        data_type=gdal.GDT_Float32,
    )
    paths = SmokeCropPaths(root, source)
    field_data = SmokeFieldData()
    AgroCropProcessor(
        make_scene(level=ProductLevel.L1C, agroids=(3,)),
        paths,
        field_data,
        SimpleNamespace(destination_srid=3857, nodata=-9999.0),
    ).run()

    for product in ("tci", "ndvi", "ndwi"):
        result = read_raster(Path(paths.destination(product, 3)))
        if result.shape != (2, 2):
            raise AssertionError(
                f"Crop {product} имеет неверный размер: {result.shape}"
            )
    if field_data.bounds_calls != 1:
        raise AssertionError(
            "Границы хозяйства были повторно прочитаны для каждого продукта"
        )


def run_mosaic_smoke(root: Path) -> None:
    """Проверяет объединение соседних тайлов и освобождение VRT."""
    root.mkdir()
    left = root / "left.tif"
    right = root / "right.tif"
    write_raster(
        left,
        np.ones((2, 2), dtype=np.float32),
        pixel_size=10,
        data_type=gdal.GDT_Float32,
    )
    write_raster(
        right,
        np.full((2, 2), 2, dtype=np.float32),
        pixel_size=10,
        data_type=gdal.GDT_Float32,
        origin_x=20,
    )
    paths = SmokeMosaicPaths(root, [left, right])
    MosaicProcessor(
        make_scene(level=ProductLevel.L1C),
        paths,
    ).run()

    result = read_raster(Path(paths.destination("ndvi")))
    if result.shape != (2, 4):
        raise AssertionError(f"Mosaic имеет неверный размер: {result.shape}")
    if not (
            np.allclose(result[:, :2], 1)
            and np.allclose(result[:, 2:], 2)
    ):
        raise AssertionError("Mosaic неверно объединил соседние тайлы")


def run_smoke_test() -> None:
    """Запускает все автономные проверки критических GDAL-операций."""
    with TemporaryDirectory(prefix="sentinel-gdal-smoke-") as temporary:
        root = Path(temporary)
        run_cloud_mask_smoke(root / "cloud-mask")
        run_spectral_index_smoke(root / "spectral-index")
        run_crop_smoke(root / "crop")
        run_mosaic_smoke(root / "mosaic")

        print("GDAL smoke-test успешно завершён")


if __name__ == "__main__":
    run_smoke_test()
