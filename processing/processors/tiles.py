"""Класс для работы с тайлами."""
import os
from pathlib import Path

from processing.domain import ProductLevel
from processing.indexes import SpectralIndexProcessor
from processing.processors.base import BaseImageProcessor
from processing.raster import translate_to_geotiff


class TileImageProcessor(BaseImageProcessor):
    """Создаёт tile-level TCI, NDVI и NDWI из каналов Sentinel."""

    PRODUCTS = frozenset({"tci", "scl", "ndvi", "ndwi"})

    def __init__(
            self,
            scene,
            paths,
            output_archive,
            options,
            products=None,
    ):
        super().__init__(scene, paths)
        self.output_archive = output_archive
        self.options = options
        self.products = frozenset(products or self.PRODUCTS)
        unknown = self.products - self.PRODUCTS
        if unknown:
            raise ValueError(
                "Неизвестные tile-продукты: " + ", ".join(sorted(unknown))
            )

    def run(self) -> None:
        """Готовит цветной растр и рассчитывает спектральные индексы тайла."""
        # 1) растерные этапы: TCI и SCL
        self._process_raster_stages()

        # 2) индексные этапы: NDVI и NDWI
        self._process_indices()

    def _process_raster_stages(self) -> None:
        """Подготовка TCI (10m) и SCL (20m) из JP2 → projection_raster."""
        stages = [
            stage
            for stage in ("tci", "scl")
            if stage in self.products and not (
                stage == "scl" and self.scene.level is ProductLevel.L1C
            )
        ]

        for stage in stages:
            stage_sources = self.paths.sources(stage)
            src = stage_sources[0] if stage_sources else None
            if src is None:
                raise FileNotFoundError(
                    f"Не найден исходник для {stage.upper()}"
                )
            self.logger.info(
                "Обработка изображения %s (%s)",
                Path(src).name, stage.upper()
            )
            dst = self.paths.destination(stage)
            if os.path.exists(dst):
                self.logger.info("%s уже есть, пропуск", stage.upper())
                self.output_archive.store(self.scene, dst, stage)
                continue

            translate_to_geotiff(src, dst)
            self.logger.info("%s готово: %s", stage.upper(), dst)

            self.output_archive.store(self.scene, dst, stage)

    def _process_indices(self) -> None:
        """Создаёт отсутствующие NDVI/NDWI с однократным чтением B08."""
        outputs = {}
        for product in ("ndvi", "ndwi"):
            if product not in self.products:
                continue
            destination = self.paths.destination(product)
            if os.path.exists(destination):
                self.logger.info("%s уже есть, пропуск", product.upper())
                self.output_archive.store(
                    self.scene,
                    destination,
                    product,
                )
            else:
                outputs[product] = destination
        if not outputs:
            return

        required_bands = {"b08"}
        if "ndvi" in outputs:
            required_bands.add("b04")
        if "ndwi" in outputs:
            required_bands.add("b03")
        sources = {}
        for band in sorted(required_bands):
            band_sources = self.paths.sources(band)
            sources[band] = band_sources[0] if band_sources else None
        missing_bands = [
            band.upper()
            for band, path in sources.items()
            if path is None
        ]
        if missing_bands:
            raise FileNotFoundError(
                "Не найдены каналы для индексов: "
                + ", ".join(missing_bands)
            )

        self.logger.info(
            "Обработка индексов %s с общим каналом B08",
            ", ".join(product.upper() for product in outputs),
        )
        SpectralIndexProcessor(
            b03_file=sources.get("b03"),
            b04_file=sources.get("b04"),
            b08_file=sources["b08"],
            nodata=self.options.nodata,
        ).create(outputs)
        for product, destination in outputs.items():
            self.logger.info(
                "%s готово: %s",
                product.upper(),
                destination,
            )
            self.output_archive.store(
                self.scene,
                destination,
                product,
            )
