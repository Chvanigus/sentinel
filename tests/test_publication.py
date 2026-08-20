
"""Тесты планирования и пакетной публикации растров."""

from datetime import UTC, date, datetime

import pytest

from core.logging import get_logger
from domain.models import LayerSourceMetadata
from satgeo import publisher as publisher_module
from satgeo.publisher import (
    PostgisPublicationRepository,
    PublicationPlanner,
    RasterPublisher,
)


def test_publish_date_raises_when_a_file_was_not_published(
        tmp_path,
):
    """Пакетная публикация сообщает обо всех неуспешных TIFF."""
    raster = tmp_path / "s2a_01_07_2026_a3_ndvi_10m_3857.tif"
    raster.write_bytes(b"not-a-real-raster")
    publisher = RasterPublisher.__new__(RasterPublisher)
    publisher.source_root = tmp_path
    publisher.logger = get_logger("test-publication")
    publisher._publish_file = lambda _path, _source=None, _quality=None: (
        False,
        "optimize_failed",
        None,
    )

    with pytest.raises(RuntimeError, match=raster.name):
        publisher.publish_date(date(2026, 7, 1))


def test_publish_date_ignores_non_tiff_files(tmp_path):
    """Служебные файлы не публикуются и не скрывают отсутствие TIFF."""
    (tmp_path / "notes.txt").write_text("metadata", encoding="utf-8")
    publisher = RasterPublisher.__new__(RasterPublisher)
    publisher.source_root = tmp_path
    publisher.logger = get_logger("test-publication-non-tiff")
    publisher._publish_file = lambda _path, _source=None: pytest.fail(
        "Служебный файл не должен публиковаться"
    )

    with pytest.raises(RuntimeError, match="Не найдены готовые TIFF"):
        publisher.publish_date(date(2026, 7, 1))


def test_publish_date_does_not_repeat_previous_dates(tmp_path):
    """Публикация даты не затрагивает оставшиеся результаты других дат."""
    previous = tmp_path / "s2a_30_06_2026_a3_ndvi_10m_3857.tif"
    current = tmp_path / "s2a_01_07_2026_a3_ndvi_10m_3857.tif"
    previous.write_bytes(b"previous")
    current.write_bytes(b"current")
    published = []
    publisher = RasterPublisher.__new__(RasterPublisher)
    publisher.source_root = tmp_path
    publisher.logger = get_logger("test-publication-date")
    publisher._publish_file = lambda path, _source=None, _quality=None: (
        published.append(path) is None,
        path.name,
        None,
    )

    publisher.publish_date(date(2026, 7, 1))

    assert published == [current]


def test_publication_planner_builds_host_and_container_paths(tmp_path):
    """Планировщик согласованно строит host- и container-пути."""
    planner = PublicationPlanner(
        tmp_path / "geoware",
        "/opt/geoserver_data/geoware",
    )

    plan = planner.build(
        tmp_path / "s2a_01_07_2026_a3_ndvi_10m_3857.tif"
    )

    assert plan.layer_name == "a3_ndvi_2026-07-01"
    assert plan.store_name == "a3_ndvi_2026-07-01_store"
    assert plan.destination == (
        tmp_path
        / "geoware"
        / "2026"
        / "a3"
        / "ndvi"
        / "07"
        / "a3_ndvi_2026-07-01.tif"
    )
    assert plan.container_path == (
        "/opt/geoserver_data/geoware/2026/a3/ndvi/07/"
        "a3_ndvi_2026-07-01.tif"
    )


def test_publication_planner_reuses_legacy_month_path(tmp_path):
    """Повторная публикация не создаёт копию между ``7`` и ``07``."""
    planner = PublicationPlanner(
        tmp_path / "geoware",
        "/opt/geoserver_data/geoware",
    )
    legacy = (
        tmp_path / "geoware" / "2026" / "a4" / "ndvi" / "7"
        / "a4_ndvi_2026-07-02.tif"
    )
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b"published")

    plan = planner.build(
        tmp_path / "s2a_02_07_2026_a4_ndvi_10m_3857.tif"
    )

    assert plan.destination == legacy
    assert plan.container_path.endswith(
        "/2026/a4/ndvi/7/a4_ndvi_2026-07-02.tif"
    )


def test_publication_persists_visual_metadata(tmp_path):
    """Публикация сохраняет метаданные источника и покрытия хозяйства."""
    source_file = tmp_path / "s2b_01_07_2026_a3_ndvi_10m_3857.tif"
    source_file.write_bytes(b"source")
    layers = []

    class Client:
        """Имитирует создание нового ресурса GeoServer."""

        def create_coveragestore(self, **_options):
            """Сообщает о создании нового coverage store."""
            return True

        def set_layer_style(self, *_args):
            """Имитирует установку стиля."""

        def enable_gwc_gridset_3857(self, _layer_name):
            """Имитирует включение WebMercator gridset."""
            return True

        def seed_gwc_cache(self, **_options):
            """Имитирует прогрев кэша."""
            return True

    class Repository:
        """Фиксирует сохраняемые метаданные слоя."""

        def add_layers(self, values):
            """Запоминает пакет опубликованных слоёв."""
            layers.extend(values)

        def quality_many(self, *, agroids, **_options):
            """Возвращает тестовые проценты покрытия хозяйств."""
            return {agroid: (12.5, 81.25) for agroid in agroids}

        def bounds(self, **_options):
            """Возвращает тестовые границы хозяйства."""
            return 1.0, 2.0, 3.0, 4.0

    acquired_at = datetime(2026, 7, 1, 8, 16, 11, tzinfo=UTC)
    publisher = RasterPublisher(
        source_root=tmp_path,
        workspace="sentinel",
        current_year=2027,
        planner=PublicationPlanner(
            tmp_path / "geoware",
            "/opt/geoserver_data/geoware",
        ),
        client=Client(),
        repository=Repository(),
        optimizer=lambda src, dst: dst.parent.mkdir(parents=True)
        or dst.write_bytes(src.read_bytes()),
    )

    publisher.publish_date(
        date(2026, 7, 1),
        LayerSourceMetadata(
            acquired_at=acquired_at,
            satellite="S2B",
            source_level="L2A",
            processing_baseline=511,
            source_tiles_by_agroid={3: ("T38ULA",)},
            algorithm_version="3.0.0",
        ),
    )

    assert len(layers) == 1
    layer = layers[0]
    assert layer.acquired_at == acquired_at
    assert layer.satellite == "S2B"
    assert layer.source_level == "L2A"
    assert layer.processing_baseline == 511
    assert layer.source_tiles == ("T38ULA",)
    assert layer.cloud_coverage_percent == 12.5
    assert layer.valid_coverage_percent == 81.25
    assert layer.resolution_m == 10
    assert layer.is_cloud_masked is False
    assert layer.algorithm_version == "3.0.0"


def test_postgis_repository_caches_repeated_agro_bounds(monkeypatch):
    """Три слоя хозяйства используют один запрос его неизменных границ."""
    calls = []

    class Connection:
        """Минимальный context manager подключения."""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class Repository:
        """Фиксирует чтение границ."""

        def __init__(self, _gateway):
            pass

        def bounds(self, **parameters):
            """Возвращает и фиксирует тестовые границы."""
            calls.append(parameters)
            return 1.0, 2.0, 3.0, 4.0

    monkeypatch.setattr(
        publisher_module.psycopg2,
        "connect",
        lambda **_options: Connection(),
    )
    monkeypatch.setattr(
        publisher_module,
        "get_database_config",
        lambda: {},
    )
    monkeypatch.setattr(publisher_module, "SqlGateway", lambda value: value)
    monkeypatch.setattr(publisher_module, "FieldRepository", Repository)
    repository = PostgisPublicationRepository()

    first = repository.bounds(year=2026, agroid=3, srid=3857)
    second = repository.bounds(year=2026, agroid=3, srid=3857)

    assert first == second == (1.0, 2.0, 3.0, 4.0)
    assert calls == [{"srid": 3857, "year": 2026, "agroid": 3}]


def test_postgis_repository_reads_all_quality_in_one_query(monkeypatch):
    """Качество нескольких хозяйств агрегируется одним подключением."""
    connections = []

    class Connection:
        """Имитирует gateway и подключение к PostGIS."""

        def __enter__(self):
            connections.append(self)
            return self

        def __exit__(self, *_args):
            return None

        def rows(self, query, params):
            """Возвращает агрегаты двух хозяйств."""
            self.query = query
            self.params = params
            return [
                {
                    "agroid": 3,
                    "cloud_pixels": 10,
                    "valid_pixels": 80,
                    "total_pixels": 100,
                },
                {
                    "agroid": 4,
                    "cloud_pixels": 5,
                    "valid_pixels": 90,
                    "total_pixels": 100,
                },
            ]

    monkeypatch.setattr(
        publisher_module.psycopg2,
        "connect",
        lambda **_options: Connection(),
    )
    monkeypatch.setattr(
        publisher_module,
        "get_database_config",
        lambda: {},
    )
    monkeypatch.setattr(publisher_module, "SqlGateway", lambda value: value)
    repository = PostgisPublicationRepository()

    first = repository.quality_many(
        year=2026,
        agroids=(3, 4),
        acquired_on=date(2026, 7, 1),
    )
    second = repository.quality_many(
        year=2026,
        agroids=(4, 3),
        acquired_on=date(2026, 7, 1),
    )

    assert first == {3: (10.0, 80.0), 4: (5.0, 90.0)}
    assert second == {4: (5.0, 90.0), 3: (10.0, 80.0)}
    assert len(connections) == 1
    assert "WITH requested" in connections[0].query
    assert connections[0].params == ([3, 4], 2026, date(2026, 7, 1))


def test_refresh_product_overwrites_cog_and_reseeds_cache(tmp_path):
    """Перерасчёт NDVI заменяет опубликованный COG и обновляет GWC-кэш."""
    source = tmp_path / "s2a_01_07_2026_a3_ndvi_10m_3857.tif"
    source.write_bytes(b"new")
    planner = PublicationPlanner(
        tmp_path / "geoware",
        "/opt/geoserver_data/geoware",
    )
    destination = planner.build(source).destination
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"old")
    optimized = []
    seed_calls = []

    class Client:
        """Фиксирует операции GeoServer и GWC."""

        def create_coveragestore(self, **_options):
            """Имитирует идемпотентную публикацию."""

        def set_layer_style(self, *_args):
            """Имитирует установку стиля."""

        def enable_gwc_gridset_3857(self, _layer_name):
            """Имитирует включение gridset."""
            return True

        def seed_gwc_cache(self, **options):
            """Запоминает режим обновления кэша."""
            seed_calls.append(options)
            return True

    class Repository:
        """Имитирует persistence публикации."""

        def bounds(self, **_options):
            """Возвращает валидные границы хозяйства."""
            return 1.0, 2.0, 3.0, 4.0

    publisher = RasterPublisher(
        source_root=tmp_path,
        workspace="sentinel",
        current_year=2027,
        planner=planner,
        client=Client(),
        repository=Repository(),
        optimizer=lambda src, dst: optimized.append((src, dst)),
        refresh_products={"ndvi"},
    )

    success, _layer_name, layer = publisher._publish_file(source)

    assert success is True
    assert layer is not None
    assert optimized == [(source, destination)]
    assert seed_calls[0]["reseed"] is True


def test_existing_layer_skips_reconfiguration_and_cache_seed(tmp_path):
    """Повторная публикация готового слоя не запускает дорогие операции GWC."""
    source = tmp_path / "s2a_01_07_2026_a3_ndwi_10m_3857.tif"
    source.write_bytes(b"source")
    planner = PublicationPlanner(
        tmp_path / "geoware",
        "/opt/geoserver_data/geoware",
    )
    destination = planner.build(source).destination
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"published")
    saved = []

    class Client:
        """Имитирует уже существующий coverage store."""

        def create_coveragestore(self, **_options):
            """Сообщает, что ресурс существовал до запуска."""
            return False

        def set_layer_style(self, *_args):
            """Запрещает повторную настройку стиля."""
            pytest.fail("Стиль существующего слоя не нужно переназначать")

        def enable_gwc_gridset_3857(self, _layer_name):
            """Запрещает повторную настройку gridset."""
            pytest.fail("Gridset существующего слоя уже настроен")

        def seed_gwc_cache(self, **_options):
            """Запрещает повторный seed неизменённого слоя."""
            pytest.fail("Неизменённый слой не нужно прогревать повторно")

    class Repository:
        """Фиксирует идемпотентное обновление записи слоя."""

        def bounds(self, **_options):
            """Запрещает лишнее чтение границ."""
            pytest.fail("Границы без seed не требуются")

    publisher = RasterPublisher(
        source_root=tmp_path,
        workspace="sentinel",
        current_year=2026,
        planner=planner,
        client=Client(),
        repository=Repository(),
        optimizer=lambda *_args: pytest.fail(
            "Существующий COG не нужно формировать повторно"
        ),
    )

    success, _layer_name, layer = publisher._publish_file(source)

    assert success is True
    assert layer is not None
    saved.append(layer)
    assert len(saved) == 1
