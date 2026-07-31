"""GeoServerClient на базе geoserver-rest (geo.Geoserver.Geoserver)."""
from __future__ import annotations

import os
from dataclasses import dataclass

import requests
from geoserver.catalog import Catalog, FailedRequestError
from geoserver.store import UnsavedCoverageStore

from core.logging import get_logger


@dataclass(frozen=True)
class GeoServerConfig:
    """Параметры подключения и рабочая область GeoServer."""

    host: str
    workspace: str
    username: str
    password: str

    @property
    def base_url(self) -> str:
        """Возвращает базовый HTTP-адрес GeoServer."""
        return f"http://{self.host}/geoserver"


class GeoServerClient:
    """Клиент для GeoServer на основе geoserver-rest."""

    def __init__(
            self,
            config: GeoServerConfig,
            http: requests.Session | None = None,
    ):
        self.config = config
        self.cat = Catalog(
            f"{config.base_url}/rest",
            config.username,
            config.password,
        )
        self.workspace = config.workspace
        self.http = http or requests.Session()
        self.logger = get_logger(__class__.__name__)

    def create_coveragestore(
            self,
            store_name: str,
            container_path: str,
            create_layer=True,
            layer_name=None,
            source_name=None,
    ):
        """
        Создаём coverage store в GeoServer.

        Почему здесь кастомная реализация:
        Стандартный метод Catalog.create_coveragestore() из библиотеки
        geoserver-restconfig содержит неочевидный баг —
        после успешного создания store он пытается вернуть связанный ресурс
        через вызов get_resources().

        Это приводит к следующему поведению:

        выполняется полный обход всех store-ов в workspace
        для каждого workspace отправляются запросы:
        GET /datastores
        GET /coveragestores
        GET /wmsstores
        затем дополнительно запрашиваются ресурсы внутри каждого store

        В результате один вызов create_coveragestore генерирует десятки
        (а иногда сотни) лишних HTTP GET-запросов.

        Текущий верхнеуровневый метод можно использовать вместо стандартного
        метода библиотеки в high-load сценариях.
        """
        store = self.cat.get_store(name=store_name, workspace=self.workspace)

        if store:
            self.logger.info(
                "%s уже существует - пропуск создания...",
                store_name
            )
            return

        self.logger.info("Создаём store: %s", store_name)

        cs = UnsavedCoverageStore(self.cat, store_name, self.workspace)
        cs.type = "GeoTIFF"
        cs.url = container_path if container_path.startswith(
            "file:") else f"file:{container_path}"

        self.cat.save(cs)

        if create_layer:
            if layer_name is None:
                layer_name = \
                    os.path.splitext(os.path.basename(container_path))[0]
            if source_name is None:
                source_name = \
                    os.path.splitext(os.path.basename(container_path))[0]

            data = (
                f"<coverage>"
                f"<name>{layer_name}</name>"
                f"<nativeName>{source_name}</nativeName>"
                f"</coverage>"
            )
            url = (
                f"{self.cat.service_url}/workspaces/{self.workspace}"
                f"/coveragestores/{store_name}/coverages.xml"
            )
            headers = {"Content-type": "application/xml"}

            resp = self.cat.http_request(url, method="post", data=data,
                                         headers=headers)
            if resp.status_code != 201:
                raise FailedRequestError(
                    f"Failed to create coverage/layer {layer_name} for {store_name}: "
                    f"{resp.status_code}, {resp.text}"
                )

        self.cat._cache.clear()

        self.logger.info("Store %s успешно создан", store_name)

    def set_layer_style(self, layer_name: str, style_name: str) -> None:
        """Устанавливаем стиль для слоя."""
        layer = self.cat.get_layer(layer_name)
        if not layer:
            raise RuntimeError(f"Слой {layer_name} не найден в GeoServer")

        layer._set_default_style(style_name)
        self.cat.save(layer)

    def enable_gwc_gridset_3857(self, layer_name: str) -> bool:
        """
        Включить тайловый кэш для слоя и задать GridSet EPSG:3857.
        """
        full_layer = f"{self.workspace}:{layer_name}"

        url = f"{self.config.base_url}/gwc/rest/layers/{full_layer}.xml"

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

        resp = self.http.put(
            url, data=payload.encode("utf-8"),
            auth=(self.config.username, self.config.password),
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

        url = f"{self.config.base_url}/gwc/rest/seed/{full_layer}.xml"

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

        resp = self.http.post(
            url,
            data=payload.encode("utf-8"),
            auth=(self.config.username, self.config.password),
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
