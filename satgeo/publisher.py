"""Application service публикации готовых растров."""
from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import psycopg2

from core.logging import get_logger
from db.connection import get_database_config
from db.gateway import SqlGateway
from db.repositories import FieldRepository, LayerRepository
from domain.models import LayerSourceMetadata, PublishedLayer

from .models import PublicationPlan, split_file_name
from .optimizer import optimize_geotiff


class PublicationRepository(Protocol):
    """Порт persistence для публикации."""

    def add_layers(self, layers: list[PublishedLayer]) -> None:
        """Регистрирует опубликованные слои одним пакетом."""
        ...

    def bounds(
            self,
            year: int,
            agroid: int,
            srid: int,
    ) -> tuple[float, float, float, float]:
        """Возвращает границы хозяйства для прогрева кэша."""
        ...

    def quality_many(
            self,
            year: int,
            agroids: tuple[int, ...],
            acquired_on: date,
    ) -> dict[int, tuple[float | None, float | None]]:
        """Возвращает качество выбранных хозяйств одним запросом."""
        ...


class GeoServerCatalog(Protocol):
    """Минимальный контракт GeoServer, необходимый publisher."""

    def create_coveragestore(
            self,
            store_name: str,
            container_path: str,
            **kwargs,
    ) -> bool:
        """Создаёт coverage store и сообщает, был ли создан новый ресурс."""
        ...

    def set_layer_style(self, layer_name: str, style_name: str) -> None:
        """Назначает слою стиль по умолчанию."""
        ...

    def enable_gwc_gridset_3857(self, layer_name: str) -> bool:
        """Включает WebMercator gridset в GeoWebCache."""
        ...

    def seed_gwc_cache(
            self,
            layer_name: str,
            bbox: tuple[float, float, float, float],
            reseed: bool = False,
            **kwargs,
    ) -> bool:
        """Запускает прогрев тайлов слоя в указанной области."""
        ...


class PublicationPlanner:
    """Чистые правила имён и путей GeoServer."""

    STYLE_MAP = {
        "ndvi": "ndvi",
        "ndwi": "ndwi",
        "scl": "scl",
        "tci": None,
    }

    def __init__(
            self,
            host_data_root: str | Path,
            container_data_root: str | Path,
    ):
        self.host_data_root = Path(host_data_root)
        self.container_data_root = Path(container_data_root)

    def build(self, source: str | Path) -> PublicationPlan:
        """Строит имена и пути публикации из имени готового растра."""
        source_path = Path(source)
        info = split_file_name(source_path.name)
        acquired_on = info.date()
        layer_name = (
            f"a{info.agroid}_{info.img_type}_{acquired_on.isoformat()}"
        )
        destination = (
            self.host_data_root
            / str(acquired_on.year)
            / f"a{info.agroid}"
            / info.img_type
            / f"{acquired_on.month:02d}"
            / f"{layer_name}.tif"
        )
        relative = destination.relative_to(self.host_data_root)
        container_path = (
            self.container_data_root / relative
        ).as_posix()
        return PublicationPlan(
            source=source_path,
            destination=destination,
            container_path=container_path,
            layer_name=layer_name,
            store_name=f"{layer_name}_store",
            style_name=self.STYLE_MAP[info.img_type],
            info=info,
        )


class PostgisPublicationRepository:
    """PostGIS-адаптер publication persistence port."""

    def __init__(self) -> None:
        """Создаёт кэш неизменных границ хозяйств одного запуска."""
        self._bounds: dict[
            tuple[int, int, int],
            tuple[float, float, float, float],
        ] = {}
        self._quality: dict[
            tuple[int, int, date],
            tuple[float | None, float | None],
        ] = {}

    def add_layers(self, layers: list[PublishedLayer]) -> None:
        """Сохраняет все слои даты одним подключением к PostGIS."""
        if not layers:
            return
        with psycopg2.connect(**get_database_config()) as connection:
            LayerRepository(SqlGateway(connection)).add_many(layers)

    def bounds(
            self,
            year: int,
            agroid: int,
            srid: int,
    ) -> tuple[float, float, float, float]:
        """Читает и кеширует границы хозяйства из PostGIS."""
        key = (year, agroid, srid)
        if key in self._bounds:
            return self._bounds[key]
        with psycopg2.connect(**get_database_config()) as connection:
            bounds = FieldRepository(SqlGateway(connection)).bounds(
                srid=srid,
                year=year,
                agroid=agroid,
            )
        self._bounds[key] = bounds
        return bounds

    def quality_many(
            self,
            year: int,
            agroids: tuple[int, ...],
            acquired_on: date,
    ) -> dict[int, tuple[float | None, float | None]]:
        """Агрегирует пиксельные показатели всех хозяйств одним SQL."""
        selected = tuple(dict.fromkeys(agroids))
        missing = [
            agroid
            for agroid in selected
            if (year, agroid, acquired_on) not in self._quality
        ]
        if not missing:
            return {
                agroid: self._quality[(year, agroid, acquired_on)]
                for agroid in selected
            }
        with psycopg2.connect(**get_database_config()) as connection:
            gateway = SqlGateway(connection)
            rows = gateway.rows(
                """
                WITH requested AS (
                    SELECT unnest(%s::integer[]) AS agroid
                )
                SELECT
                    requested.agroid,
                    SUM(ndvi.cloud_pixel_count) AS cloud_pixels,
                    SUM(ndvi.valid_pixel_count) AS valid_pixels,
                    SUM(ndvi.total_pixel_count) AS total_pixels
                FROM requested
                LEFT JOIN LATERAL
                    gpgeo.__geo_get_fieldnames_for_agro_year(
                        requested.agroid,
                        %s
                    ) AS field
                    ON true
                LEFT JOIN gpgeo.maps_ndvi_values AS ndvi
                    ON ndvi.fieldid = field.id
                    AND ndvi.date = %s
                GROUP BY requested.agroid
                """,
                (missing, year, acquired_on),
            )
        by_agroid = {row["agroid"]: row for row in rows}
        for agroid in missing:
            row = by_agroid.get(agroid)
            total = int(row["total_pixels"] or 0) if row else 0
            cloud = row["cloud_pixels"] if row else None
            valid = row["valid_pixels"] if row else None
            self._quality[(year, agroid, acquired_on)] = (
                float(cloud) / total * 100
                if cloud is not None and total
                else None,
                float(valid) / total * 100
                if valid is not None and total
                else None,
            )
        return {
            agroid: self._quality[(year, agroid, acquired_on)]
            for agroid in selected
        }


class RasterPublisher:
    """Координирует публикацию, используя внедрённые адаптеры."""

    def __init__(
            self,
            source_root: str | Path,
            workspace: str,
            current_year: int,
            planner: PublicationPlanner,
            client: GeoServerCatalog,
            repository: PublicationRepository,
            optimizer: Callable[[Path, Path], None] = optimize_geotiff,
            refresh_products: Iterable[str] = (),
    ):
        self.source_root = Path(source_root)
        self.workspace = workspace
        self.current_year = current_year
        self.planner = planner
        self.client = client
        self.repository = repository
        self.optimizer = optimizer
        self.refresh_products = frozenset(
            product.lower() for product in refresh_products
        )
        self.logger = get_logger(self.__class__.__name__)

    def _publish_file(
            self,
            file_path: Path,
            source: LayerSourceMetadata | None = None,
            quality: tuple[float | None, float | None] = (None, None),
    ) -> tuple[bool, str, PublishedLayer | None]:
        """Оптимизирует и публикует файл, возвращая запись для БД."""
        plan = self.planner.build(file_path)
        refresh = plan.info.img_type in self.refresh_products
        try:
            if refresh or not plan.destination.exists():
                self.optimizer(plan.source, plan.destination)
        except Exception as exc:
            self.logger.error(
                "Ошибка оптимизации %s: %s",
                file_path,
                exc,
                exc_info=True,
            )
            return False, f"optimize_failed: {file_path.name}", None

        created = self.client.create_coveragestore(
            store_name=plan.store_name,
            container_path=plan.container_path,
            layer_name=plan.layer_name,
            source_name=plan.layer_name,
        )
        if created and plan.style_name:
            self.client.set_layer_style(
                plan.layer_name,
                plan.style_name,
            )

        cloud_percent, valid_percent = quality
        layer = PublishedLayer(
            name=f"{self.workspace}:{plan.layer_name}",
            acquired_on=plan.info.date(),
            product=plan.info.img_type,
            agroid=plan.info.agroid_number,
            acquired_at=source.acquired_at if source else None,
            satellite=(
                source.satellite
                if source
                else plan.info.satellite.upper()
            ),
            source_level=source.source_level if source else None,
            processing_baseline=(
                source.processing_baseline if source else None
            ),
            source_tiles=(
                source.source_tiles_by_agroid.get(
                    plan.info.agroid_number,
                    (),
                )
                if source
                else ()
            ),
            cloud_coverage_percent=cloud_percent,
            valid_coverage_percent=valid_percent,
            resolution_m=plan.info.resolution,
            is_cloud_masked=False,
            algorithm_version=(
                source.algorithm_version if source else None
            ),
            generated_at=datetime.now(UTC),
        )
        if created:
            self.client.enable_gwc_gridset_3857(plan.layer_name)

        acquired_on = plan.info.date()
        if refresh or (
                created and self.current_year == acquired_on.year
        ):
            bbox = self.repository.bounds(
                year=acquired_on.year,
                agroid=plan.info.agroid_number,
                srid=3857,
            )
            self.client.seed_gwc_cache(
                layer_name=plan.layer_name,
                zoom_start=8,
                zoom_stop=14,
                threads=4,
                image_format="image/png",
                bbox=bbox,
                reseed=refresh,
            )
        return True, plan.layer_name, layer

    def publish_date(
            self,
            acquired_on: date,
            source: LayerSourceMetadata | None = None,
    ) -> None:
        """Публикует TIFF указанной даты и агрегирует ошибки."""
        failures = []
        matched_files = []
        for root, _, files in os.walk(self.source_root):
            for filename in files:
                if not filename.lower().endswith(".tif"):
                    continue
                file_path = Path(root) / filename
                try:
                    if split_file_name(filename).date() != acquired_on:
                        continue
                    matched_files.append(file_path)
                except Exception as exc:
                    self.logger.exception(
                        "Ошибка публикации %s: %s",
                        filename,
                        exc,
                    )
                    failures.append(filename)

        if not matched_files:
            raise RuntimeError(
                "Не найдены готовые TIFF за дату "
                f"{acquired_on.isoformat()}"
            )

        quality_by_agroid = {}
        if source is not None:
            agroids = tuple(dict.fromkeys(
                split_file_name(path.name).agroid_number
                for path in matched_files
            ))
            quality_by_agroid = self.repository.quality_many(
                year=acquired_on.year,
                agroids=agroids,
                acquired_on=acquired_on,
            )

        published_layers = []
        for file_path in matched_files:
            filename = file_path.name
            agroid = split_file_name(filename).agroid_number
            try:
                success, reason, layer = self._publish_file(
                    file_path,
                    source,
                    quality_by_agroid.get(agroid, (None, None)),
                )
            except Exception as exc:
                self.logger.exception(
                    "Ошибка публикации %s: %s",
                    filename,
                    exc,
                )
                failures.append(filename)
                continue
            if not success:
                self.logger.warning(
                    "Файл не опубликован: %s (%s)",
                    filename,
                    reason,
                )
                failures.append(filename)
            elif layer is not None:
                published_layers.append(layer)

        if published_layers:
            self.repository.add_layers(published_layers)
        if failures:
            raise RuntimeError(
                "Не удалось опубликовать файлы: " + ", ".join(failures)
            )
