"""Класс для сбора статистики NDVI по спутниковым снимкам."""
from __future__ import annotations

import os
from time import perf_counter

from domain.models import Field
from processing.ndvi import NdviFieldAnalyzer
from processing.ports import FieldDataProvider
from processing.processors.base import BaseImageProcessor
from processing.raster import clip_by_mask_array
from processing.storage import FieldGeometryExporter


class NdviStatisticsProcessor(BaseImageProcessor):
    """Класс для сбора и анализа статистики NDVI по спутниковым снимкам."""

    def __init__(self,
                 scene,
                 paths,
                 field_data: FieldDataProvider,
                 geometry_exporter: FieldGeometryExporter,
                 nodata: float,
                 overwrite: bool = False,
                 ) -> None:
        super().__init__(scene, paths)
        self.field_data = field_data
        self.geometry_exporter = geometry_exporter
        self.nodata = nodata
        self.overwrite = overwrite
        self.analyzer = NdviFieldAnalyzer(nodata_value=nodata)

    def _save_field_geojsons(
            self,
            fields: list[Field],
            agroid: int,
    ) -> None:
        """Пакетно получает и сохраняет отсутствующие GeoJSON-маски."""
        missing = [
            field
            for field in fields
            if not os.path.exists(
                self.paths.field_geojson(agroid, field.name)
            )
        ]
        if not missing:
            return

        field_ids = []
        for field in missing:
            if field.id is None:
                raise ValueError(f"У поля {field.name} отсутствует id")
            field_ids.append(field.id)

        geometries = self.field_data.geometries(
            field_ids=field_ids,
            year=self.scene.acquired_on.year,
        )
        for field in missing:
            try:
                geometry = geometries[field.id]
            except KeyError as exc:
                raise LookupError(
                    f"Не найдена геометрия поля {field.id}"
                ) from exc
            self.geometry_exporter.export(
                geometry,
                self.paths.field_geojson(agroid, field.name),
            )

    def run(self) -> None:
        """Рассчитывает и сохраняет NDVI-статистику всех доступных полей."""
        for agroid in self.scene.agroids:
            agro_started = perf_counter()
            src_ndvi = self.paths.ndvi_source(agroid)
            self.logger.info("Агро %s: проверка %s", agroid, src_ndvi)
            if not os.path.exists(src_ndvi):
                self.logger.warning("NDVI не найден → пропуск: %s", src_ndvi)
                continue

            year = self.scene.acquired_on.year
            if not self.overwrite and self.field_data.ndvi_is_complete(
                    agroid=agroid,
                    year=year,
                    acquired_on=self.scene.acquired_on,
            ):
                self.logger.info(
                    "Агро %s: NDVI уже рассчитан за %s — пропуск",
                    agroid, self.scene.date_label
                )
                continue

            fields = self.field_data.fields(
                agroid=agroid,
                year=year,
            )
            self.logger.info(
                "Агро %s: полей для анализа — %d",
                agroid,
                len(fields),
            )
            self._save_field_geojsons(fields, agroid)
            ndvi_values = []

            for field in fields:
                if field.id is None:
                    raise ValueError(f"У поля {field.name} отсутствует id")
                geojson = self.paths.field_geojson(agroid, field.name)
                if not os.path.exists(geojson):
                    self.logger.warning("GeoJSON не найден → %s", geojson)
                    continue

                val = self.analyzer.analyze(
                    ndvi=clip_by_mask_array(
                        src_ndvi,
                        geojson,
                        x_resolution=10,
                        y_resolution=10,
                        nodata=self.nodata,
                    ),
                    acquired_on=self.scene.acquired_on,
                    field_id=field.id,
                )
                if val is not None:
                    ndvi_values.append(val)

            field_ids = [
                field.id
                for field in fields
                if field.id is not None
            ]
            if ndvi_values or self.overwrite:
                self.logger.info(
                    "Агро %s: %s %s записей в БД",
                    agroid,
                    (
                        "полностью заменяем"
                        if self.overwrite
                        else "сохраняем"
                    ),
                    len(ndvi_values),
                )
                self.field_data.save_ndvi(
                    ndvi_values,
                    field_ids=field_ids,
                    acquired_on=self.scene.acquired_on,
                    overwrite=self.overwrite,
                )
            self.logger.info(
                "NDVI STATS OK: агро=%s полей=%d значений=%d | %.2f сек.",
                agroid,
                len(fields),
                len(ndvi_values),
                perf_counter() - agro_started,
            )
