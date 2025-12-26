"""Базовые классы."""
import requests
from geoserver.catalog import Catalog

from core import settings
from core.logging import get_logger


class GeoServerBase:
    """Базовый REST-клиент для работы с GeoServer."""
    def __init__(self):
        self.logger = get_logger()
        self._rest_url = f"http://{settings.RMHOST}:{settings.TSPORT}/geoserver/rest/"
        self._session = self._make_session()
        self.catalog = Catalog(self._rest_url, settings.TSUSER, settings.TSPASSWORD)
    
    @staticmethod
    def _make_session() -> requests.Session:
        """Создание сессии."""
        sess = requests.Session()
        sess.auth = (settings.TSUSER, settings.TSPASSWORD)
        return sess

    def delete(self, path: str, params=None, headers=None) -> requests.Response:
        """DELETE запрос."""
        url = self._rest_url + path
        return self._session.delete(url, params=params, headers=headers)

    def post(self, path: str, xml: str, headers=None) -> requests.Response:
        """POST запрос."""
        url = self._rest_url + path
        return self._session.post(url, data=xml, headers=headers)

    def save_catalog(self, obj) -> None:
        """Сохранение каталога."""
        self.catalog.save(obj)
