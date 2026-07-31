"""Тесты orchestration обработки одного Sentinel-тайла."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from processing.domain import ProductLevel, SceneContext
from processing.processors.tiles import TileImageProcessor


class RecordingPaths:
    """Предоставляет настроенные пути и фиксирует запросы процессора."""

    def __init__(self, destinations, sources):
        """Сохраняет таблицы результатов и исходных каналов."""
        self.destinations = destinations
        self.source_paths = sources
        self.source_requests = []

    def destination(self, product):
        """Возвращает настроенный путь результата."""
        return str(self.destinations[product])

    def sources(self, band):
        """Возвращает исходники канала и фиксирует обращение."""
        self.source_requests.append(band)
        return self.source_paths[band]


class RecordingArchive:
    """Фиксирует результаты, переданные в долговременный архив."""

    def __init__(self):
        """Создаёт пустой журнал операций архивирования."""
        self.stores = []

    def store(self, scene, source, product):
        """Запоминает сцену, исходный путь и тип продукта."""
        self.stores.append((scene, source, product))


class RecordingRasterProcessor:
    """Имитирует GDAL-конвертацию растровых каналов."""

    conversions = []

    def __init__(self, source, destination):
        """Сохраняет источник и назначение будущей конвертации."""
        self.source = source
        self.destination = destination

    def translate_to_geotiff(self):
        """Запоминает запрос конвертации вместо вызова GDAL."""
        self.conversions.append((self.source, self.destination))


class RecordingIndexProcessor:
    """Имитирует общий расчёт выбранных спектральных индексов."""

    initializations = []
    creations = []

    def __init__(self, **options):
        """Запоминает каналы и параметры расчёта."""
        self.initializations.append(options)

    def create(self, outputs):
        """Запоминает набор запрошенных выходных индексов."""
        self.creations.append(outputs.copy())


def make_scene(level=ProductLevel.L2A):
    """Создаёт контекст тестовой Sentinel-сцены."""
    return SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=level,
        agroids=(1, 3, 4),
    )


def destinations(tmp_path):
    """Формирует пути всех tile-level результатов."""
    return {
        product: tmp_path / f"{product}.tif"
        for product in ("tci", "scl", "ndvi", "ndwi")
    }


def test_run_converts_rasters_calculates_indices_and_archives_all(
        tmp_path,
        monkeypatch,
):
    """Полный L2A-запуск создаёт и архивирует четыре tile-продукта."""
    result_paths = destinations(tmp_path)
    paths = RecordingPaths(
        result_paths,
        {
            "tci": ["tci.jp2"],
            "scl": ["scl.jp2"],
            "b03": ["b03.jp2"],
            "b04": ["b04.jp2"],
            "b08": ["b08.jp2"],
        },
    )
    archive = RecordingArchive()
    RecordingRasterProcessor.conversions = []
    RecordingIndexProcessor.initializations = []
    RecordingIndexProcessor.creations = []
    monkeypatch.setattr(
        "processing.processors.tiles.RasterProcessor",
        RecordingRasterProcessor,
    )
    monkeypatch.setattr(
        "processing.processors.tiles.SpectralIndexProcessor",
        RecordingIndexProcessor,
    )
    current_scene = make_scene()

    TileImageProcessor(
        current_scene,
        paths,
        archive,
        SimpleNamespace(nodata=-42.0),
    ).run()

    assert RecordingRasterProcessor.conversions == [
        ("tci.jp2", str(result_paths["tci"])),
        ("scl.jp2", str(result_paths["scl"])),
    ]
    assert RecordingIndexProcessor.initializations == [
        {
            "b03_file": "b03.jp2",
            "b04_file": "b04.jp2",
            "b08_file": "b08.jp2",
            "nodata": -42.0,
        }
    ]
    assert RecordingIndexProcessor.creations == [
        {
            "ndvi": str(result_paths["ndvi"]),
            "ndwi": str(result_paths["ndwi"]),
        }
    ]
    assert [(source, product) for _, source, product in archive.stores] == [
        (str(result_paths["tci"]), "tci"),
        (str(result_paths["scl"]), "scl"),
        (str(result_paths["ndvi"]), "ndvi"),
        (str(result_paths["ndwi"]), "ndwi"),
    ]


def test_indices_resume_archives_existing_and_reads_only_needed_bands(
        tmp_path,
        monkeypatch,
):
    """Возобновление считает только NDVI и не запрашивает ненужный B03."""
    result_paths = destinations(tmp_path)
    result_paths["ndwi"].touch()
    paths = RecordingPaths(
        result_paths,
        {
            "b04": ["b04.jp2"],
            "b08": ["b08.jp2"],
        },
    )
    archive = RecordingArchive()
    RecordingIndexProcessor.initializations = []
    RecordingIndexProcessor.creations = []
    monkeypatch.setattr(
        "processing.processors.tiles.SpectralIndexProcessor",
        RecordingIndexProcessor,
    )
    current_scene = make_scene()

    TileImageProcessor(
        current_scene,
        paths,
        archive,
        SimpleNamespace(nodata=-9999.0),
    )._process_indices()

    assert paths.source_requests == ["b04", "b08"]
    assert RecordingIndexProcessor.initializations == [
        {
            "b03_file": None,
            "b04_file": "b04.jp2",
            "b08_file": "b08.jp2",
            "nodata": -9999.0,
        }
    ]
    assert RecordingIndexProcessor.creations == [
        {"ndvi": str(result_paths["ndvi"])}
    ]
    assert [(source, product) for _, source, product in archive.stores] == [
        (str(result_paths["ndwi"]), "ndwi"),
        (str(result_paths["ndvi"]), "ndvi"),
    ]


def test_indices_report_all_missing_bands_in_stable_order(tmp_path):
    """Ошибка отсутствующих каналов перечисляет их детерминированно."""
    paths = RecordingPaths(
        destinations(tmp_path),
        {
            "b03": [],
            "b04": [],
            "b08": [],
        },
    )

    with pytest.raises(
            FileNotFoundError,
            match=r"B03, B04, B08$",
    ):
        TileImageProcessor(
            make_scene(),
            paths,
            RecordingArchive(),
            SimpleNamespace(nodata=-9999.0),
        )._process_indices()


def test_l1c_raster_stage_does_not_request_scl(tmp_path):
    """L1C-конвертация обрабатывает TCI и не запрашивает отсутствующий SCL."""
    result_paths = destinations(tmp_path)
    result_paths["tci"].touch()
    paths = RecordingPaths(
        result_paths,
        {"tci": ["tci.jp2"]},
    )
    archive = RecordingArchive()
    current_scene = make_scene(ProductLevel.L1C)

    TileImageProcessor(
        current_scene,
        paths,
        archive,
        SimpleNamespace(nodata=-9999.0),
    )._process_raster_stages()

    assert paths.source_requests == ["tci"]
    assert archive.stores == [
        (current_scene, str(result_paths["tci"]), "tci")
    ]
