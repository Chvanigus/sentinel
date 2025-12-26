"""Клиентская часть Geoserver."""
from .base import GeoServerBase

from core import settings
from core.logging import get_logger


def _format_path(path: str) -> str:
    """
    Помогает собрать полный REST-путь для GeoServer.
    """
    return f"{path}"  # TODO при необходимости добавить базовый URL


class GeoServerClient(GeoServerBase):
    """
    Обёртка над geoserver.catalog и REST API GeoServer.

    Предоставляет методы для управления workspace, stores и coverage,
    а также для управления GWC кешем.

    Attributes:
        workspace (str): Название рабочего пространства GeoServer.
        logger (logging.Logger): Логгер для вывода информации и ошибок.
    """

    def __init__(self):
        """
        Инициализирует GeoServerClient.

        Получает базовую авторизацию и REST-сессию из GeoServerBase,
        настраивает workspace.
        """
        super().__init__()
        self.logger = get_logger()
        self.workspace = settings.WORKSPACE

    def ensure_workspace(self):
        """
        Убеждается, что workspace существует в GeoServer.

        Возвращает объект Workspace.

        :return: Объект Workspace из geoserver.catalog
        """
        self.logger.info(f"Проверка workspace: '{self.workspace}'")
        ws = self.catalog.get_workspace(self.workspace)
        if not ws:
            self.logger.info(
                f"Workspace '{self.workspace}' не найден, создаём..."
            )
            ws = self.catalog.create_workspace(self.workspace)
            self.logger.info(f"Workspace '{self.workspace}' создан.")
        else:
            self.logger.debug(f"Workspace '{self.workspace}' уже существует.")
        return ws

    def delete_store(self, store_name: str) -> bool:
        """
        Удаляет существующий coverage-store вместе со всеми ресурсами.

        :param store_name: Имя покрытия (coverage-store) для удаления.
        :return: True, если удаление прошло успешно, иначе False.
        """
        path = f"workspaces/{self.workspace}/coveragestores/{store_name}"
        self.logger.info(
            f"Удаление store '{store_name}' в workspace '{self.workspace}'"
        )
        resp = self.delete(path,
                           params={"recurse": "true", "purge": "all"},
                           headers=settings.HEADERS_XML)
        if resp.ok:
            self.logger.info(f"Store '{store_name}' успешно удалён.")
        else:
            self.logger.warning(
                f"Не удалось удалить store '{store_name}'. HTTP {resp.status_code}"
            )
            self.logger.error(resp.text)
        return resp.ok

    def create_store(self, store_name: str, file_path: str):
        """
        Создаёт новый coverage-store на основе GeoTIFF файла.

        :param store_name: Имя нового coverage-store.
        :param file_path: Абсолютный путь к GeoTIFF файлу на сервере.
        :return: Объект Store после сохранения.
        """
        self.logger.info(
            f"Создание store '{store_name}' с файлом '{file_path}'"
        )
        store = self.catalog.create_coveragestore2(name=store_name,
                                                   workspace=self.workspace)
        store.type = "GeoTIFF"
        store.url = f"file://{file_path}"
        self.save_catalog(store)
        self.logger.info(f"Store '{store_name}' создан, URL={store.url}")
        return self.catalog.get_store(store_name, self.workspace)

    def create_coverage(self, store, coverage_name: str):
        """
        Создаёт coverage (слой) в заданном store, если он ещё не существует.

        :param store: Объект Store, в котором создаётся coverage.
        :param coverage_name: Имя coverage и слоя.
        :return: Объект Layer или None, если ошибка.
        """
        self.logger.info(f"Проверка существования coverage '{coverage_name}'")
        layer = self.catalog.get_layer(coverage_name)
        if layer:
            self.logger.debug(f"Coverage '{coverage_name}' уже существует.")
            return layer

        xml = f"""
        <coverage>
          <name>{coverage_name}</name>
          <title>{coverage_name}</title>
          <srs>EPSG:{settings.DESTSRID}</srs>
          <parameters>
            <entry><string>SUGGESTED_TILE_SIZE</string>
            <string>{settings.TILE_SIZE},{settings.TILE_SIZE}</string>
            </entry>
          </parameters>
        </coverage>"""
        path = f"workspaces/{self.workspace}/coveragestores/{store.name}/coverages"
        self.logger.info(
            f"Создание coverage '{coverage_name}' в store '{store.name}'"
        )
        resp = self.post(path, xml, headers=settings.HEADERS_XML)
        if not resp.ok:
            self.logger.error(
                f"Ошибка создания coverage '{coverage_name}': HTTP {resp.status_code}")
            return None
        self.logger.info(f"Coverage '{coverage_name}' успешно создан.")
        return self.catalog.get_layer(coverage_name)

    def seed_gwc(self, layer_name: str, zoom_start: int, zoom_stop: int):
        """
        Запускает процесс автоматического кеширования (reseed) через GWC.

        :param layer_name: Имя слоя для кеширования.
        :param zoom_start: Начальный уровень зума.
        :param zoom_stop: Конечный уровень зума.
        """
        self.logger.info(
            f"Запуск reseed GWC для '{layer_name}' ({zoom_start}-{zoom_stop})")
        xml = f"""
        <seedRequest>
          <name>{self.workspace}:{layer_name}</name>
          <srs><number>{settings.DESTSRID}</number></srs>
          <zoomStart>{zoom_start}</zoomStart>
          <zoomStop>{zoom_stop}</zoomStop>
          <format>image/png8</format>
          <type>reseed</type>
          <threadCount>4</threadCount>
        </seedRequest>"""
        path = f"gwc/rest/seed/{self.workspace}:{layer_name}.xml"
        self.post(path, xml, headers=settings.HEADERS_XML)
        self.logger.info(f"GWC reseed для '{layer_name}' инициирован.")
