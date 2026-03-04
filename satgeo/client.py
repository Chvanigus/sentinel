"""GeoServerClient на базе geoserver-rest (geo.Geoserver.Geoserver)."""

import requests
from geoserver.catalog import Catalog

from core import settings
from core.logging import get_logger


class GeoServerClient:
    """Клиент для GeoServer на основе geoserver-rest."""

    def __init__(self):
        self.cat = Catalog(
            f"http://{settings.GS_HOST}/geoserver/rest",
            settings.GS_USERNAME, settings.GS_PASSWORD
        )
        self.workspace = settings.GS_WORKSPACE
        self.logger = get_logger(__class__.__name__)

    def get_or_create_store(
            self,
            store_name: str,
            container_path: str,
    ):
        """Создаём или возвращаем store."""
        store = self.cat.get_store(store_name, workspace=self.workspace)
        if store:
            return store

        self.logger.info("Создаём store: %s", store_name)

        store = self.cat.create_coveragestore(
            name=store_name,
            workspace=self.workspace,
            path=container_path,
        )
        store.type = "GeoTIFF"
        store.url = f"file://{container_path}"
        self.cat.save(store)

        self.logger.info(f"Store %s успешно создан", store_name)

        return store

    def set_layer_style(self, layer_name: str, style_name: str) -> None:
        """Устанавливаем стиль для слоя."""
        layer = self.cat.get_layer(layer_name)
        if not layer:
            raise RuntimeError(
                "Слой %s не найден в GeoServer", layer_name
            )

        layer._set_default_style(style_name)
        self.cat.save(layer)

    def enable_gwc_gridset_3857(self, layer_name: str) -> bool:
        """
        Включить тайловый кэш для слоя и задать GridSet EPSG:3857.
        """
        full_layer = f"{self.workspace}:{layer_name}"

        url = f"http://{settings.GS_HOST}/geoserver/gwc/rest/layers/{full_layer}.xml"

        payload = f"""<?xml version="1.0" encoding="UTF-8"?>
        <GeoServerLayer>
          <name>{full_layer}</name>
          <enabled>true</enabled>
          <gridSubsets>
            <gridSubset>
              <gridSetName>WebMercatorQuad</gridSetName>
            </gridSubset>
          </gridSubsets>
          <metaWidthHeight>
            <int>1</int>
            <int>1</int>
          </metaWidthHeight>
          <mimeFormats>
            <string>image/png</string>
          </mimeFormats>
        </GeoServerLayer>
        """

        headers = {"Content-Type": "application/xml"}

        resp = requests.put(
            url, data=payload.encode("utf-8"),
            auth=(settings.GS_USERNAME, settings.GS_PASSWORD),
            headers=headers, timeout=30
        )

        if resp.status_code in (200, 201, 204):
            self.logger.info(
                "GWC: успешно включён кеш для %s (GridSet EPSG:3857)",
                layer_name
            )
            return True

        raise RuntimeError(
            f"GWC GridSet установка провалена: {resp.status_code} {resp.text}"
        )

    def seed_gwc_cache(
            self,
            layer_name: str,
            bbox: tuple[float, float, float, float],
            zoom_start: int = 0,
            zoom_stop: int = 14,
            image_format: str = "image/png",
            threads: int = 4,
    ) -> bool:
        """
        Прогрев тайлов GeoWebCache (seed), чтобы фронт сразу получал готовый кэш.
        """
        if zoom_start < 0 or zoom_stop < 0 or zoom_start > zoom_stop:
            raise ValueError("Неверный диапазон zoom_start/zoom_stop")

        minx, miny, maxx, maxy = bbox
        if not (minx < maxx and miny < maxy):
            raise ValueError(f"Неверный bbox: {bbox}")

        full_layer = f"{self.workspace}:{layer_name}"

        url = f"http://{settings.GS_HOST}/geoserver/gwc/rest/seed/{full_layer}.xml"

        payload = f"""<?xml version="1.0" encoding="UTF-8"?>
                <seedRequest>
                  <name>{full_layer}</name>
                  <gridSetId>WebMercatorQuad</gridSetId>
                  <zoomStart>{zoom_start}</zoomStart>
                  <zoomStop>{zoom_stop}</zoomStop>
                  <type>seed</type>
                  <format>{image_format}</format>
                  <threadCount>{threads}</threadCount>
                  <metaWidthHeight>
                    <int>8</int>
                    <int>8</int>
                  </metaWidthHeight>
                  <bounds>
                    <coords>
                      <double>{minx}</double>
                      <double>{miny}</double>
                      <double>{maxx}</double>
                      <double>{maxy}</double>
                    </coords>
                  </bounds>
                </seedRequest>
                """

        headers = {"Content-Type": "application/xml"}

        self.logger.info(
            "GWC SEED bbox: %s zoom=%s-%s bbox=%s",
            full_layer,
            zoom_start,
            zoom_stop,
            bbox,
        )

        resp = requests.post(
            url,
            data=payload.encode("utf-8"),
            auth=(settings.GS_USERNAME, settings.GS_PASSWORD),
            headers=headers,
            timeout=600,
        )

        if resp.status_code in (200, 201, 202):
            self.logger.info(
                "GWC SEED bbox успешно запущен: %s", full_layer
            )
            return True

        raise RuntimeError(
            f"GWC bbox seed ошибка: {resp.status_code} {resp.text}"
        )
