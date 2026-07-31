"""Application service публикации готовых растров."""
from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from datetime import date
from pathlib import Path
from typing import Protocol

import psycopg2

from core.logging import get_logger
from db.connection import get_database_config
from db.gateway import SqlGateway
from db.repositories import FieldRepository, LayerRepository
from domain.models import PublishedLayer

from .models import PublicationPlan, split_file_name
from .optimizer import optimize_geotiff


class PublicationRepository(Protocol):
    """Порт persistence для публикации."""

    def add_layer(self, layer: PublishedLayer) -> None:
        """Регистрирует опубликованный слой."""
        ...

    def bounds(
            self,
            year: int,
            agroid: int,
            srid: int,
    ) -> tuple[float, float, float, float]:
        """Возвращает границы хозяйства для прогрева кэша."""
        ...


class GeoServerCatalog(Protocol):
    """Минимальный контракт GeoServer, необходимый publisher."""

    def create_coveragestore(
            self,
            store_name: str,
            container_path: str,
            **kwargs,
    ) -> None:
        """Создаёт coverage store и связанный растровый слой."""
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

    def add_layer(self, layer: PublishedLayer) -> None:
        """Сохраняет сведения об опубликованном слое в PostGIS."""
        with psycopg2.connect(**get_database_config()) as connection:
            LayerRepository(SqlGateway(connection)).add(layer)

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

    def _publish_file(self, file_path: Path) -> tuple[bool, str]:
        """Оптимизирует, публикует и регистрирует один растровый файл."""
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
            return False, f"optimize_failed: {file_path.name}"

        self.client.create_coveragestore(
            store_name=plan.store_name,
            container_path=plan.container_path,
            layer_name=plan.layer_name,
            source_name=plan.layer_name,
        )
        if plan.style_name:
            self.client.set_layer_style(
                plan.layer_name,
                plan.style_name,
            )

        self.repository.add_layer(
            PublishedLayer(
                name=f"{self.workspace}:{plan.layer_name}",
                acquired_on=plan.info.date(),
                product=plan.info.img_type,
                agroid=plan.info.agroid_number,
            )
        )
        self.client.enable_gwc_gridset_3857(plan.layer_name)

        acquired_on = plan.info.date()
        if refresh or self.current_year == acquired_on.year:
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
        return True, plan.layer_name

    def publish_date(self, acquired_on: date) -> None:
        """Публикует TIFF указанной даты и агрегирует ошибки."""
        failures = []
        for root, _, files in os.walk(self.source_root):
            for filename in files:
                if not filename.lower().endswith(".tif"):
                    continue
                file_path = Path(root) / filename
                try:
                    if split_file_name(filename).date() != acquired_on:
                        continue
                    success, reason = self._publish_file(file_path)
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

        if failures:
            raise RuntimeError(
                "Не удалось опубликовать файлы: " + ", ".join(failures)
            )
