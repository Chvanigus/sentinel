"""Repositories предметных данных Sentinel."""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

from domain.models import (
    Field,
    LayerMetadataUpdate,
    NdviStatistics,
    PublishedLayer,
)

from .gateway import SqlGateway
from .models import NdviRecord


class LayerRepository:
    """Чтение и сохранение опубликованных слоёв."""

    REQUIRED_SETS = frozenset({"ndvi", "ndwi", "tci"})
    REQUIRED_AGROIDS = (1, 3, 4, 5, 6)

    def __init__(self, gateway: SqlGateway):
        self.gateway = gateway

    def add_many(self, layers: list[PublishedLayer]) -> None:
        """Одним запросом создаёт или обновляет опубликованные слои."""
        if not layers:
            return
        payload = json.dumps(
            [
                {
                    "acquired_on": layer.acquired_on.isoformat(),
                    "product": layer.product,
                    "agroid": layer.agroid,
                    "name": layer.name,
                    "acquired_at": (
                        layer.acquired_at.isoformat()
                        if layer.acquired_at is not None
                        else None
                    ),
                    "satellite": layer.satellite,
                    "source_level": layer.source_level,
                    "processing_baseline": layer.processing_baseline,
                    "source_tiles": layer.source_tiles,
                    "cloud_coverage_percent": (
                        layer.cloud_coverage_percent
                    ),
                    "valid_coverage_percent": (
                        layer.valid_coverage_percent
                    ),
                    "resolution_m": layer.resolution_m,
                    "is_cloud_masked": layer.is_cloud_masked,
                    "algorithm_version": layer.algorithm_version,
                    "generated_at": (
                        layer.generated_at or datetime.now(UTC)
                    ).isoformat(),
                }
                for layer in layers
            ],
            ensure_ascii=False,
        )
        self.gateway.execute(
            """
            WITH source AS (
                SELECT *
                FROM jsonb_to_recordset(%s::jsonb) AS item(
                    acquired_on date,
                    product varchar,
                    agroid integer,
                    name varchar,
                    acquired_at timestamptz,
                    satellite varchar,
                    source_level varchar,
                    processing_baseline integer,
                    source_tiles jsonb,
                    cloud_coverage_percent double precision,
                    valid_coverage_percent double precision,
                    resolution_m smallint,
                    is_cloud_masked boolean,
                    algorithm_version varchar,
                    generated_at timestamptz
                )
            )
            INSERT INTO gpgeo.maps_layer (
                date, fieldid, set, agroid, name, acquired_at, satellite,
                source_level, processing_baseline, source_tiles,
                cloud_coverage_percent, valid_coverage_percent,
                resolution_m, is_cloud_masked, algorithm_version, generated_at
            )
            SELECT
                acquired_on,
                NULL,
                product,
                agroid,
                name,
                acquired_at,
                satellite,
                source_level,
                processing_baseline,
                NULLIF(
                    ARRAY(
                        SELECT jsonb_array_elements_text(source_tiles)
                    ),
                    ARRAY[]::text[]
                ),
                cloud_coverage_percent,
                valid_coverage_percent,
                resolution_m,
                is_cloud_masked,
                algorithm_version,
                generated_at
            FROM source
            ON CONFLICT (name) DO UPDATE SET
                acquired_at = EXCLUDED.acquired_at,
                satellite = EXCLUDED.satellite,
                source_level = EXCLUDED.source_level,
                processing_baseline = EXCLUDED.processing_baseline,
                source_tiles = EXCLUDED.source_tiles,
                cloud_coverage_percent = EXCLUDED.cloud_coverage_percent,
                valid_coverage_percent = EXCLUDED.valid_coverage_percent,
                resolution_m = EXCLUDED.resolution_m,
                is_cloud_masked = EXCLUDED.is_cloud_masked,
                algorithm_version = EXCLUDED.algorithm_version,
                generated_at = EXCLUDED.generated_at
            """,
            (payload,),
        )

    def missing_agroids(self, acquired_on: date) -> list[int]:
        """Возвращает хозяйства без полного набора обязательных слоёв."""
        return self.missing_agroids_many([acquired_on])[acquired_on]

    def missing_agroids_many(
            self,
            acquired_dates: list[date],
    ) -> dict[date, list[int]]:
        """Одним запросом возвращает незавершённые хозяйства нескольких дат."""
        dates = list(dict.fromkeys(acquired_dates))
        if not dates:
            return {}
        query = """
            SELECT DISTINCT date, agroid, set
            FROM gpgeo.maps_layer
            WHERE date = ANY (%s) AND agroid = ANY (%s)
        """
        rows = self.gateway.rows(
            query,
            (dates, list(self.REQUIRED_AGROIDS)),
        )
        found: dict[tuple[date, int], set[str]] = {}
        for row in rows:
            found.setdefault(
                (row["date"], row["agroid"]),
                set(),
            ).add(row["set"])
        return {
            acquired_on: [
                agroid
                for agroid in self.REQUIRED_AGROIDS
                if not self.REQUIRED_SETS.issubset(
                    found.get((acquired_on, agroid), set())
                )
            ]
            for acquired_on in dates
        }

    def refresh_metadata(
            self,
            updates: list[LayerMetadataUpdate],
    ) -> int:
        """Одним запросом обновляет метаданные существующих визуальных слоёв."""
        if not updates:
            return 0
        payload = json.dumps(
            [
                {
                    "acquired_on": item.acquired_on.isoformat(),
                    "agroid": item.agroid,
                    "acquired_at": item.acquired_at.isoformat(),
                    "satellite": item.satellite,
                    "source_level": item.source_level,
                    "processing_baseline": item.processing_baseline,
                    "source_tiles": item.source_tiles,
                    "fallback_algorithm_version": (
                        item.fallback_algorithm_version
                    ),
                    "resolution_m": item.resolution_m,
                    "is_cloud_masked": item.is_cloud_masked,
                }
                for item in updates
            ],
            ensure_ascii=False,
        )
        row = self.gateway.row(
            """
            WITH source AS (
                SELECT *
                FROM jsonb_to_recordset(%s::jsonb) AS item(
                    acquired_on date,
                    agroid integer,
                    acquired_at timestamptz,
                    satellite varchar,
                    source_level varchar,
                    processing_baseline integer,
                    source_tiles jsonb,
                    fallback_algorithm_version varchar,
                    resolution_m smallint,
                    is_cloud_masked boolean
                )
            ),
            quality AS (
                SELECT
                    source.acquired_on,
                    source.agroid,
                    SUM(ndvi.cloud_pixel_count) AS cloud_pixels,
                    SUM(ndvi.valid_pixel_count) AS valid_pixels,
                    SUM(ndvi.total_pixel_count) AS total_pixels
                FROM source
                LEFT JOIN LATERAL
                    gpgeo.__geo_get_fieldnames_for_agro_year(
                        source.agroid,
                        EXTRACT(YEAR FROM source.acquired_on)::integer
                    ) AS field
                    ON true
                LEFT JOIN gpgeo.maps_ndvi_values AS ndvi
                    ON ndvi.date = source.acquired_on
                    AND ndvi.fieldid = field.id
                GROUP BY source.acquired_on, source.agroid
            ),
            updated AS (
                UPDATE gpgeo.maps_layer AS layer
                SET
                    acquired_at = source.acquired_at,
                    satellite = source.satellite,
                    source_level = source.source_level,
                    processing_baseline = source.processing_baseline,
                    source_tiles = NULLIF(
                        ARRAY(
                            SELECT jsonb_array_elements_text(
                                source.source_tiles
                            )
                        ),
                        ARRAY[]::text[]
                    ),
                    cloud_coverage_percent = CASE
                        WHEN quality.total_pixels > 0
                             AND quality.cloud_pixels IS NOT NULL
                        THEN quality.cloud_pixels::double precision
                             / quality.total_pixels * 100
                        ELSE NULL
                    END,
                    valid_coverage_percent = CASE
                        WHEN quality.total_pixels > 0
                             AND quality.valid_pixels IS NOT NULL
                        THEN quality.valid_pixels::double precision
                             / quality.total_pixels * 100
                        ELSE NULL
                    END,
                    resolution_m = source.resolution_m,
                    is_cloud_masked = source.is_cloud_masked,
                    algorithm_version = COALESCE(
                        layer.algorithm_version,
                        source.fallback_algorithm_version
                    )
                FROM source
                JOIN quality
                    ON quality.acquired_on = source.acquired_on
                    AND quality.agroid = source.agroid
                WHERE layer.date = source.acquired_on
                  AND layer.agroid = source.agroid
                RETURNING layer.id
            )
            SELECT COUNT(*) AS updated_count FROM updated
            """,
            (payload,),
        )
        return int(row["updated_count"] if row else 0)


class FieldRepository:
    """Доступ к полям и их геометрии."""

    def __init__(self, gateway: SqlGateway):
        self.gateway = gateway

    def list_for_agro(self, agroid: int, year: int) -> list[Field]:
        """Возвращает поля хозяйства для указанного сезона."""
        rows = self.gateway.rows(
            "SELECT * FROM gpgeo.__geo_get_fieldnames_for_agro_year(%s, %s)",
            (agroid, year),
        )
        return [
            Field(id=int(row["id"]), name=str(row["name"]))
            for row in rows
        ]

    def bounds(
            self,
            srid: int,
            year: int,
            agroid: int | None = None,
            field_id: int | None = None,
    ) -> tuple[float, float, float, float]:
        """Возвращает прямоугольные границы хозяйства или отдельного поля."""
        rows = self.gateway.rows(
            "SELECT * FROM gpgeo.__geostl_get_boundpoints(%s, %s, %s, %s)",
            (year, srid, field_id, agroid),
        )
        if len(rows) < 3:
            raise LookupError(
                f"Не найдены границы: agroid={agroid}, field={field_id}"
            )
        return rows[0][0], rows[0][1], rows[2][0], rows[2][1]

    def geometries(
            self,
            field_ids: list[int],
            year: int,
    ) -> dict[int, Any]:
        """Возвращает геометрии полей одним запросом к PostGIS."""
        if not field_ids:
            return {}
        rows = self.gateway.rows(
            """
            SELECT requested.fieldid, shape.*
            FROM unnest(%s::integer[]) AS requested(fieldid)
            CROSS JOIN LATERAL gpgeo.__geo_get_field_shape(
                requested.fieldid,
                %s
            ) AS shape
            """,
            (field_ids, year),
        )
        result = {row[0]: row[1] for row in rows}
        missing = set(field_ids).difference(result)
        if missing:
            raise LookupError(
                "Не найдены геометрии полей: "
                + ", ".join(str(field_id) for field_id in sorted(missing))
            )
        return result


class NdviRepository:
    """Хранение статистики NDVI."""

    def __init__(self, gateway: SqlGateway):
        self.gateway = gateway

    @staticmethod
    def _rows(values: list[NdviStatistics]) -> list[tuple[Any, ...]]:
        """Преобразует доменную статистику в строки persistence-модели."""
        return [
            (
                item.acquired_on,
                item.field_id,
                item.mean,
                item.maximum,
                item.minimum,
                item.growth_percent,
                item.coefficient_of_variation,
                item.is_uniform,
                item.valid_pixel_count,
                item.total_pixel_count,
                item.cloud_pixel_count,
                item.nodata_pixel_count,
                item.shadow_pixel_count,
                item.snow_pixel_count,
                item.valid_coverage_percent,
                item.cloud_coverage_percent,
                item.standard_deviation,
                item.median,
                item.percentile_10,
                item.percentile_90,
                item.source_level,
                item.algorithm_version,
                item.calculated_at,
            )
            for item in values
        ]

    def add_many(self, values: list[NdviStatistics]) -> None:
        """Сохраняет пакет доменных значений статистики NDVI."""
        self.gateway.insert_many(
            NdviRecord,
            self._rows(values),
            conflict_fields="date, fieldid",
        )

    def replace_many(
            self,
            values: list[NdviStatistics],
            *,
            field_ids: list[int],
            acquired_on: date,
    ) -> None:
        """Атомарно заменяет статистику выбранных полей за одну дату."""
        selected_ids = sorted(set(field_ids))
        if not selected_ids:
            return
        if any(
                item.acquired_on != acquired_on
                or item.field_id not in selected_ids
                for item in values
        ):
            raise ValueError(
                "Заменяемые NDVI-значения не соответствуют дате или полям"
            )

        rows = self._rows(values)
        self.gateway.execute(
            """
            DELETE FROM gpgeo.maps_ndvi_values
            WHERE date = %s AND fieldid = ANY (%s)
            """,
            (acquired_on, selected_ids),
            commit=not rows,
        )
        if rows:
            self.gateway.insert_many(
                NdviRecord,
                rows,
                conflict_fields="date, fieldid",
            )

    def is_complete(
            self,
            fields: list[Field],
            acquired_on: date,
    ) -> bool:
        """Проверяет наличие статистики за дату для каждого переданного поля."""
        field_ids = [field.id for field in fields if field.id is not None]
        if not field_ids:
            return False
        rows = self.gateway.rows(
            """
            SELECT DISTINCT fieldid
            FROM gpgeo.maps_ndvi_values
            WHERE date = %s AND fieldid = ANY (%s)
            """,
            (acquired_on, field_ids),
        )
        existing = {row["fieldid"] for row in rows}
        return set(field_ids).issubset(existing)
