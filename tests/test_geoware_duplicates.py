"""Тесты безопасного аудита дублей конечного хранилища geoware."""
from pathlib import Path

import pytest

from scripts.geoware_duplicates import (
    StoreReference,
    apply_cleanup,
    build_cleanup_decisions,
    discover_duplicate_groups,
)


def create_raster(root: Path, month: str) -> Path:
    """Создаёт тестовую копию одного NDVI-растра в каталоге месяца."""
    path = (
        root / "2026" / "a4" / "ndvi" / month
        / "a4_ndvi_2026-07-02.tif"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(month.encode())
    return path


@pytest.mark.parametrize(
    ("active_month", "removed_month"),
    [("7", "07"), ("07", "7")],
)
def test_cleanup_keeps_file_referenced_by_geoserver(
        tmp_path,
        active_month,
        removed_month,
):
    """Удаляется только копия, на которую GeoServer не ссылается."""
    legacy = create_raster(tmp_path, "7")
    canonical = create_raster(tmp_path, "07")
    groups = discover_duplicate_groups(tmp_path)
    references = {
        "a4_ndvi_2026-07-02_store": StoreReference(
            url=(
                "file:/opt/geoserver_data/geoware/2026/a4/ndvi/"
                f"{active_month}/"
                "a4_ndvi_2026-07-02.tif"
            ),
        ),
    }

    decisions = build_cleanup_decisions(
        groups,
        references,
        host_root=tmp_path,
        container_root="/opt/geoserver_data/geoware",
    )
    removed = apply_cleanup(decisions, tmp_path)

    assert removed == 1
    by_month = {"7": legacy, "07": canonical}
    assert by_month[active_month].exists()
    assert not by_month[removed_month].exists()


def test_cleanup_skips_group_when_store_path_is_unknown(tmp_path):
    """При недоступном store обе копии остаются нетронутыми."""
    legacy = create_raster(tmp_path, "7")
    canonical = create_raster(tmp_path, "07")
    groups = discover_duplicate_groups(tmp_path)

    decisions = build_cleanup_decisions(
        groups,
        {groups[0].store_name: StoreReference(error="unavailable")},
        host_root=tmp_path,
        container_root="/opt/geoserver_data/geoware",
    )
    removed = apply_cleanup(decisions, tmp_path)

    assert removed == 0
    assert legacy.exists()
    assert canonical.exists()
    assert decisions[0].status == "skipped"
