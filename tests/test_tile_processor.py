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


RASTER_CONVERSIONS = []


def recording_translate(source, destination):
    """Запоминает запрос конвертации вместо вызова GDAL."""
    RASTER_CONVERSIONS.append((source, destination))


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


def test_run_converts_rasters_and_calculates_all_indices(
        tmp_path,
        monkeypatch,
):
    """Полный L2A-запуск создаёт четыре рабочих tile-продукта."""
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
    RASTER_CONVERSIONS.clear()
    RecordingIndexProcessor.initializations = []
    RecordingIndexProcessor.creations = []
    monkeypatch.setattr(
        "processing.processors.tiles.translate_to_geotiff",
        recording_translate,
    )
    monkeypatch.setattr(
        "processing.processors.tiles.SpectralIndexProcessor",
        RecordingIndexProcessor,
    )
    current_scene = make_scene()

    TileImageProcessor(
        current_scene,
        paths,
        SimpleNamespace(nodata=-42.0),
    ).run()

    assert RASTER_CONVERSIONS == [
        ("tci.jp2", str(result_paths["tci"])),
        ("scl.jp2", str(result_paths["scl"])),
    ]
    assert RecordingIndexProcessor.initializations == [
        {
            "b03_file": "b03.jp2",
            "b04_file": "b04.jp2",
            "b08_file": "b08.jp2",
            "b03_offset": 0.0,
            "b04_offset": 0.0,
            "b08_offset": 0.0,
            "nodata": -42.0,
        }
    ]
    assert RecordingIndexProcessor.creations == [
        {
            "ndvi": str(result_paths["ndvi"]),
            "ndwi": str(result_paths["ndwi"]),
        }
    ]
def test_indices_resume_reads_only_needed_bands(
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
        SimpleNamespace(nodata=-9999.0),
    )._process_indices()

    assert paths.source_requests == ["b04", "b08"]
    assert RecordingIndexProcessor.initializations == [
        {
            "b03_file": None,
            "b04_file": "b04.jp2",
            "b08_file": "b08.jp2",
            "b03_offset": 0.0,
            "b04_offset": 0.0,
            "b08_offset": 0.0,
            "nodata": -9999.0,
        }
    ]
    assert RecordingIndexProcessor.creations == [
        {"ndvi": str(result_paths["ndvi"])}
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
    current_scene = make_scene(ProductLevel.L1C)

    TileImageProcessor(
        current_scene,
        paths,
        SimpleNamespace(nodata=-9999.0),
    )._process_raster_stages()

    assert paths.source_requests == ["tci"]


def test_ndvi_only_mode_does_not_read_unrelated_l1c_bands(
        tmp_path,
        monkeypatch,
):
    """NDVI-перерасчёт L1C читает только красный и ближний ИК-каналы."""
    result_paths = destinations(tmp_path)
    paths = RecordingPaths(
        result_paths,
        {
            "b04": ["b04.jp2"],
            "b08": ["b08.jp2"],
        },
    )
    RecordingIndexProcessor.initializations = []
    RecordingIndexProcessor.creations = []
    monkeypatch.setattr(
        "processing.processors.tiles.SpectralIndexProcessor",
        RecordingIndexProcessor,
    )

    TileImageProcessor(
        make_scene(ProductLevel.L1C),
        paths,
        SimpleNamespace(nodata=-9999.0),
        products={"ndvi", "scl"},
    ).run()

    assert paths.source_requests == ["b04", "b08"]
    assert RecordingIndexProcessor.creations == [
        {"ndvi": str(result_paths["ndvi"])}
    ]
