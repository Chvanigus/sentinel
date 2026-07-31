
"""Тесты планирования и пакетной публикации растров."""

from datetime import date

import pytest

from core.logging import get_logger
from satgeo.publisher import PublicationPlanner, RasterPublisher


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
