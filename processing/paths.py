"""Чистые правила путей processing pipeline."""
from __future__ import annotations

from glob import glob

from .domain import ProductLevel, SceneContext
from .workspace import WorkspacePaths


class ScenePaths:
    """Общий контекст path resolver без stage-string интерфейса."""

    def __init__(
            self,
            scene: SceneContext,
            workspace: WorkspacePaths,
    ):
        self.scene = scene
        self.workspace = workspace

    def _name(self, suffix: str) -> str:
        """Формирует имя результата из спутника, даты и суффикса."""
        return f"{self.scene.satellite}_{self.scene.date_label}_{suffix}"


class L2AProductPaths(ScenePaths):
    """Исходные SAFE-файлы и tile-level результаты L2A."""

    _BANDS = {
        "tci": "TCI",
        "scl": "SCL",
        "b03": "B03",
        "b04": "B04",
        "b08": "B08",
    }

    def sources(self, band: str) -> list[str]:
        """Находит исходники канала L2A в распакованной SAFE-структуре."""
        try:
            band_name = self._BANDS[band]
        except KeyError as exc:
            raise ValueError(f"Неизвестный канал L2A: {band}") from exc
        resolution = 20 if band == "scl" else 10
        tile = self.scene.tile.upper()
        pattern = (
            self.workspace.temporary
            / f"{self.scene.satellite.upper()}_MSIL2A*{tile}*"
            / "GRANULE"
            / f"L2A_{tile}*"
            / "IMG_DATA"
            / f"R{resolution}m"
            / f"{tile}*{band_name}_{resolution}m.jp2"
        )
        return sorted(glob(str(pattern)))

    def destination(self, product: str) -> str:
        """Возвращает путь tile-level результата в исходной проекции."""
        return str(
            self.workspace.intermediate
            / (
                f"{self.scene.satellite}_{self.scene.tile}_"
                f"{self.scene.date_label}_{product}_native.tif"
            )
        )


class L1CProductPaths(ScenePaths):
    """Исходные SAFE-файлы и tile-level результаты L1C."""

    _BANDS = {
        "tci": "TCI",
        "b03": "B03",
        "b04": "B04",
        "b08": "B08",
    }

    def sources(self, band: str) -> list[str]:
        """Находит исходники канала L1C в распакованной SAFE-структуре."""
        if band == "scl":
            return []
        try:
            band_name = self._BANDS[band]
        except KeyError as exc:
            raise ValueError(f"Неизвестный канал L1C: {band}") from exc
        tile = self.scene.tile.upper()
        pattern = (
            self.workspace.temporary
            / f"{self.scene.satellite.upper()}_MSIL1C*{tile}*"
            / "GRANULE"
            / f"L1C_{tile}*"
            / "IMG_DATA"
            / f"{tile}*_{band_name}.jp2"
        )
        return sorted(glob(str(pattern)))

    def destination(self, product: str) -> str:
        """Возвращает путь tile-level результата L1C в исходной проекции."""
        return str(
            self.workspace.intermediate
            / (
                f"{self.scene.satellite}_{self.scene.tile}_"
                f"{self.scene.date_label}_{product}_native.tif"
            )
        )


class SentinelCropPaths(ScenePaths):
    """Tile-level источники и результаты вырезки по агропредприятиям."""

    _SIZES = {"tci": 10, "ndvi": 10, "ndwi": 10, "scl": 20}

    def sources(self, product: str) -> list[str]:
        """Возвращает tile-level растр продукта для вырезки."""
        if product not in self._SIZES:
            raise ValueError(f"Неизвестный продукт: {product}")
        path = (
            self.workspace.intermediate
            / (
                f"{self.scene.satellite}_{self.scene.tile}_"
                f"{self.scene.date_label}_{product}_native.tif"
            )
        )
        return sorted(glob(str(path)))

    def destination(self, product: str, agroid: int) -> str:
        """Строит путь результата хозяйства с фактическим разрешением и SRID."""
        if product == "scl" and self.scene.level is ProductLevel.L1C:
            raise ValueError("SCL отсутствует у продукта L1C")
        try:
            size = self._SIZES[product]
        except KeyError as exc:
            raise ValueError(f"Неизвестный продукт: {product}") from exc

        root = (
            self.workspace.intermediate
            if agroid == 1 or size == 20
            else self.workspace.processed
        )
        tile_suffix = f"_{self.scene.tile}" if agroid == 1 else ""
        return str(
            root
            / (
                f"{self.scene.satellite}_{self.scene.date_label}_a{agroid}_"
                f"{product}_{size}m_3857{tile_suffix}.tif"
            )
        )


class MosaicPaths(ScenePaths):
    """Источники отдельных тайлов и объединённые результаты."""

    _SIZES = {"tci": 10, "scl": 20, "ndvi": 10, "ndwi": 10}

    def sources(self, product: str, agroid: int = 1) -> list[str]:
        """Находит фрагменты соседних тайлов для мозаики хозяйства."""
        try:
            size = self._SIZES[product]
        except KeyError as exc:
            raise ValueError(f"Неизвестный продукт: {product}") from exc
        pattern = (
            self.workspace.intermediate
            / (
                f"*_{self.scene.date_label}_a{agroid}_"
                f"{product}_{size}m_*.tif"
            )
        )
        return sorted(glob(str(pattern)))

    def destination(self, product: str) -> str:
        """Возвращает путь объединённого растра продукта."""
        try:
            size = self._SIZES[product]
        except KeyError as exc:
            raise ValueError(f"Неизвестный продукт: {product}") from exc
        root = (
            self.workspace.intermediate
            if size == 20
            else self.workspace.processed
        )
        return str(
            root
            / self._name(f"a1_{product}_{size}m_3857.tif")
        )


class CloudMaskPaths(ScenePaths):
    """Пути ресемплинга SCL и фильтрации NDVI."""

    def ndvi(self, agroid: int) -> str:
        """Возвращает путь исходного NDVI хозяйства."""
        return str(
            self.workspace.processed
            / self._name(f"a{agroid}_ndvi_10m_3857.tif")
        )

    def scl_20m(self, agroid: int) -> str:
        """Возвращает путь исходной SCL-маски с разрешением 20 метров."""
        return str(
            self.workspace.intermediate
            / self._name(f"a{agroid}_scl_20m_3857.tif")
        )

    def scl_10m(self, agroid: int) -> str:
        """Возвращает путь ресемплированной SCL-маски."""
        return str(
            self.workspace.processed
            / self._name(f"a{agroid}_scl_10m_3857.tif")
        )

    def filtered_ndvi(self, agroid: int) -> str:
        """Возвращает путь NDVI после фильтрации облачности."""
        return str(
            self.workspace.intermediate
            / self._name(f"a{agroid}_ndvi_10m_3857_filtered.tif")
        )


class NdviStatisticsPaths(ScenePaths):
    """Источники NDVI и временные файлы статистики полей."""

    def ndvi_source(self, agroid: int) -> str:
        """Выбирает корректный NDVI-источник с учётом уровня продукта."""
        if self.scene.level is ProductLevel.L1C:
            root = self.workspace.processed
            suffix = "ndvi_10m_3857.tif"
        else:
            root = self.workspace.intermediate
            suffix = "ndvi_10m_3857_filtered.tif"
        return str(root / self._name(f"a{agroid}_{suffix}"))

    def field_geojson(self, agroid: int, field_code: str) -> str:
        """Возвращает путь временной GeoJSON-маски поля."""
        return str(
            self.workspace.ndvi
            / f"A{agroid}_{self.scene.date_label}_FIELD{field_code}.geojson"
        )

    def field_ndvi_tif(self, agroid: int, field_code: str) -> str:
        """Возвращает путь вырезанного по полю NDVI-растра."""
        return str(
            self.workspace.ndvi
            / (
                f"A{agroid}_{self.scene.date_label}_"
                f"FIELD{field_code}_ndvi.tif"
            )
        )
