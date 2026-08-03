"""Тесты правил формирования путей processing pipeline."""

from datetime import date
from pathlib import Path

import pytest

from processing.domain import ProductLevel, SceneContext
from processing.paths import (
    CloudMaskPaths,
    L1CProductPaths,
    L2AProductPaths,
    MosaicPaths,
    NdviStatisticsPaths,
    SentinelCropPaths,
)
from processing.workspace import WorkspacePaths


def scene(level: ProductLevel) -> SceneContext:
    """Создаёт тестовый контекст сцены заданного уровня."""
    return SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=level,
        agroids=(1, 3, 4),
    )


def workspace(tmp_path: Path) -> WorkspacePaths:
    """Создаёт изолированное рабочее пространство теста."""
    return WorkspacePaths(
        temporary=tmp_path / "temp",
        intermediate=tmp_path / "intermediate",
        processed=tmp_path / "processed",
        ndvi=tmp_path / "ndvi",
        geoware=tmp_path / "geoware",
    )


def create_file(path: Path) -> Path:
    """Создаёт пустой файл вместе с родительскими каталогами."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return path


def test_l2a_product_paths_resolve_safe_bands_and_destination(tmp_path):
    """L2A resolver находит каналы разного разрешения внутри SAFE."""
    paths = L2AProductPaths(
        scene(ProductLevel.L2A),
        workspace(tmp_path),
    )
    red = create_file(
        tmp_path
        / "temp"
        / "S2A_MSIL2A_TEST_T38ULA_TEST"
        / "GRANULE"
        / "L2A_T38ULA_TEST"
        / "IMG_DATA"
        / "R10m"
        / "T38ULA_TEST_B04_10m.jp2"
    )
    scl = create_file(
        tmp_path
        / "temp"
        / "S2A_MSIL2A_TEST_T38ULA_TEST"
        / "GRANULE"
        / "L2A_T38ULA_TEST"
        / "IMG_DATA"
        / "R20m"
        / "T38ULA_TEST_SCL_20m.jp2"
    )

    assert paths.sources("b04") == [str(red)]
    assert paths.sources("scl") == [str(scl)]
    assert Path(paths.destination("ndvi")) == (
        tmp_path
        / "intermediate"
        / "s2a_t38ula_01_07_2026_ndvi_native.tif"
    )
    with pytest.raises(ValueError, match="Неизвестный канал L2A"):
        paths.sources("b12")


def test_l1c_product_paths_resolve_safe_band_without_scl(tmp_path):
    """L1C resolver учитывает структуру SAFE и отсутствие SCL."""
    paths = L1CProductPaths(
        scene(ProductLevel.L1C),
        workspace(tmp_path),
    )
    red = create_file(
        tmp_path
        / "temp"
        / "S2A_MSIL1C_TEST_T38ULA_TEST"
        / "GRANULE"
        / "L1C_T38ULA_TEST"
        / "IMG_DATA"
        / "T38ULA_TEST_B04.jp2"
    )

    assert paths.sources("b04") == [str(red)]
    assert paths.sources("scl") == []
    assert Path(paths.destination("tci")) == (
        tmp_path
        / "intermediate"
        / "s2a_t38ula_01_07_2026_tci_native.tif"
    )
    with pytest.raises(ValueError, match="Неизвестный канал L1C"):
        paths.sources("b12")


def test_crop_paths_select_storage_by_agroid_and_resolution(tmp_path):
    """Пути кропов разделяют временные и итоговые продукты."""
    current_workspace = workspace(tmp_path)
    paths = SentinelCropPaths(
        scene(ProductLevel.L2A),
        current_workspace,
    )
    ndvi = create_file(
        current_workspace.intermediate
        / "s2a_t38ula_01_07_2026_ndvi_native.tif"
    )

    assert paths.sources("ndvi") == [str(ndvi)]
    assert Path(paths.destination("ndvi", 1)) == (
        current_workspace.intermediate
        / "s2a_01_07_2026_a1_ndvi_10m_3857_t38ula.tif"
    )
    assert Path(paths.destination("ndvi", 3)) == (
        current_workspace.processed
        / "s2a_01_07_2026_a3_ndvi_10m_3857.tif"
    )
    assert Path(paths.destination("scl", 3)) == (
        current_workspace.intermediate
        / "s2a_01_07_2026_a3_scl_20m_3857.tif"
    )
    with pytest.raises(ValueError, match="Неизвестный продукт"):
        paths.sources("b04")
    with pytest.raises(ValueError, match="Неизвестный продукт"):
        paths.destination("b04", 3)


def test_l1c_crop_rejects_scl_destination(tmp_path):
    """Пути кропов запрещают несуществующую SCL-маску продукта L1C."""
    paths = SentinelCropPaths(
        scene(ProductLevel.L1C),
        workspace(tmp_path),
    )

    with pytest.raises(ValueError, match="SCL отсутствует"):
        paths.destination("scl", 3)


def test_mosaic_paths_sort_tiles_and_select_destination_root(tmp_path):
    """Пути мозаики сортируют тайлы и разделяют продукты по каталогам."""
    current_workspace = workspace(tmp_path)
    paths = MosaicPaths(
        scene(ProductLevel.L2A),
        current_workspace,
    )
    second = create_file(
        current_workspace.intermediate
        / "s2b_01_07_2026_a1_ndvi_10m_3857_t38ulb.tif"
    )
    first = create_file(
        current_workspace.intermediate
        / "s2a_01_07_2026_a1_ndvi_10m_3857_t38ula.tif"
    )

    assert paths.sources("ndvi") == [str(first), str(second)]
    assert Path(paths.destination("ndvi")) == (
        current_workspace.processed
        / "s2a_01_07_2026_a1_ndvi_10m_3857.tif"
    )
    assert Path(paths.destination("scl")) == (
        current_workspace.intermediate
        / "s2a_01_07_2026_a1_scl_20m_3857.tif"
    )
    with pytest.raises(ValueError, match="Неизвестный продукт"):
        paths.sources("b04")
    with pytest.raises(ValueError, match="Неизвестный продукт"):
        paths.destination("b04")


def test_cloud_mask_paths_form_rescale_contract(tmp_path):
    """SCL-resolver согласует имена исходной и приведённой масок."""
    current_workspace = workspace(tmp_path)
    paths = CloudMaskPaths(
        scene(ProductLevel.L2A),
        current_workspace,
    )

    assert Path(paths.ndvi(4)) == (
        current_workspace.processed
        / "s2a_01_07_2026_a4_ndvi_10m_3857.tif"
    )
    assert Path(paths.scl_20m(4)) == (
        current_workspace.intermediate
        / "s2a_01_07_2026_a4_scl_20m_3857.tif"
    )
    assert Path(paths.scl_10m(4)) == (
        current_workspace.processed
        / "s2a_01_07_2026_a4_scl_10m_3857.tif"
    )


def test_statistics_paths_form_field_mask(tmp_path):
    """Пути статистики формируют согласованное имя временной маски."""
    current_workspace = workspace(tmp_path)
    paths = NdviStatisticsPaths(
        scene(ProductLevel.L2A),
        current_workspace,
    )

    assert Path(paths.field_geojson(3, "42")) == (
        current_workspace.ndvi
        / "A3_01_07_2026_FIELD42.geojson"
    )


def test_l1c_statistics_uses_unfiltered_processed_ndvi(tmp_path):
    """Статистика L1C читает исходный NDVI без облачной маски."""
    paths = NdviStatisticsPaths(
        scene(ProductLevel.L1C),
        workspace(tmp_path),
    )

    assert Path(paths.ndvi_source(3)) == (
        tmp_path
        / "processed"
        / "s2a_01_07_2026_a3_ndvi_10m_3857.tif"
    )


def test_l2a_statistics_uses_visual_ndvi_and_separate_scl(tmp_path):
    """Статистика L2A читает визуальный NDVI и отдельную SCL-маску."""
    paths = NdviStatisticsPaths(
        scene(ProductLevel.L2A),
        workspace(tmp_path),
    )

    assert Path(paths.ndvi_source(1)) == (
        tmp_path
        / "processed"
        / "s2a_01_07_2026_a1_ndvi_10m_3857.tif"
    )
    assert Path(paths.scl_source(1)) == (
        tmp_path
        / "processed"
        / "s2a_01_07_2026_a1_scl_10m_3857.tif"
    )
