
"""Тесты планирования и пакетной публикации растров."""

from datetime import date

import pytest

from core.logging import get_logger
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
    publisher._publish_file = lambda _path: (False, "optimize_failed")

    with pytest.raises(RuntimeError, match=raster.name):
        publisher.publish_date(date(2026, 7, 1))


def test_publish_date_ignores_non_tiff_files(tmp_path):
    """Служебные файлы не передаются в публикацию."""
    (tmp_path / "notes.txt").write_text("metadata", encoding="utf-8")
    publisher = RasterPublisher.__new__(RasterPublisher)
    publisher.source_root = tmp_path
    publisher.logger = get_logger("test-publication-non-tiff")
    publisher._publish_file = lambda _path: pytest.fail(
        "Служебный файл не должен публиковаться"
    )

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
    publisher._publish_file = lambda path: (
        published.append(path) is None,
        path.name,
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

        def add_layer(self, _layer):
            """Имитирует сохранение слоя."""

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

    success, _layer_name = publisher._publish_file(source)

    assert success is True
    assert optimized == [(source, destination)]
    assert seed_calls[0]["reseed"] is True
